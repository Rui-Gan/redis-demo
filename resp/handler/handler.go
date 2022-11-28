package handler

import (
	"context"
	"io"
	"net"
	"redis/cluster"
	"redis/config"
	"redis/database"
	databaseface "redis/interface/database"
	"redis/lib/logger"
	"redis/lib/sync/atomic"
	"redis/resp/connection"
	"redis/resp/parser"
	"redis/resp/reply"
	"strings"
	"sync"
)

var (
	unKnownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type RespHandler struct {
	activeConn sync.Map
	db         databaseface.Database
	closing    atomic.Boolean
}

func MakeHandler() *RespHandler {
	var db databaseface.Database
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		db = cluster.MakeClusterDatabase()
	} else {
		db = database.NewStandaloneDatabase()
	}

	return &RespHandler{
		db: db,
	}
}

func (r *RespHandler) CloseClient(client *connection.Connection) {
	_ = client.Close()
	r.db.AfterClientClose(client)
	r.activeConn.Delete(client)
}

func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if r.closing.Get() {
		_ = conn.Close()
	}
	client := connection.NewConn(conn)
	r.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				r.CloseClient(client)
				logger.Info("connection closed" + client.RemoteAddr().String())
				return
			}
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				r.CloseClient(client)
				logger.Info("connection closed" + client.RemoteAddr().String())
				return
			}
			continue
		}

		if payload.Data == nil {
			continue
		}
		reply, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := r.db.Exec(client, reply.Args)
		if result != nil {
			client.Write(result.ToBytes())
		} else {
			client.Write(unKnownErrReplyBytes)
		}
	}
}

func (r *RespHandler) Close() error {
	logger.Info("handler shutting down")
	r.closing.Set(true)
	r.activeConn.Range(
		func(key interface{}, value interface{}) bool {
			client := key.(*connection.Connection)
			_ = client.Close()
			return true
		})
	r.db.Close()
	return nil
}
