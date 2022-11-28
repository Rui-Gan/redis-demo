package database

import (
	"redis/interface/resp"
	"redis/lib/utils"
	"redis/lib/wildcard"
	"redis/resp/reply"
)

func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine2("del", keys...))
	}
	return reply.MakeIntReply(int64(deleted))
}

func execExists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

func execFlushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	db.addAof(utils.ToCmdLine2("fushdb", keys...))
	return reply.MakeOkReply()
}

func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")

	}
	return &reply.UnKnownErrReply{}
}

func execRename(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dst := string(args[1])
	entity, exists := db.GetEntity(src)
	if !exists {
		reply.MakeErrReply("no such key")
	}
	db.PutEntity(dst, entity)
	db.Removes(src)
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	db.addAof(utils.ToCmdLine2("rename", keys...))
	return reply.MakeOkReply()
}

func execRenamenx(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dst := string(args[1])
	_, ok := db.GetEntity(dst)
	if ok {
		return reply.MakeIntReply(0)
	}
	entity, exists := db.GetEntity(src)
	if !exists {
		reply.MakeErrReply("no such key")
	}
	db.PutEntity(dst, entity)
	db.Removes(src)
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	db.addAof(utils.ToCmdLine2("renamenx", keys...))

	return reply.MakeIntReply(1)
}

func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("compile pattern error")
	}
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

func init() {
	RegisterCommand("DEL", execDel, -2)
	RegisterCommand("Exists", execExists, -2)
	RegisterCommand("flushdb", execFlushDB, -1)
	RegisterCommand("Type", execType, 2)
	RegisterCommand("RENAME", execRename, 3)
	RegisterCommand("RENAMENX", execRenamenx, 3)
	RegisterCommand("KEYS", execKeys, 2)
}
