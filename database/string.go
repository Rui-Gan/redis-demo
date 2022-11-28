package database

import (
	"redis/interface/database"
	"redis/interface/resp"
	"redis/lib/utils"
	"redis/resp/reply"
)

func execGet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeBulkReply(bytes)
}

func execSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	db.addAof(utils.ToCmdLine2("set", keys...))

	return reply.MakeOkReply()
}

func execSetnx(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	db.addAof(utils.ToCmdLine2("setnx", keys...))

	return reply.MakeIntReply(int64(result))
}

func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity, exists := db.GetEntity(key)
	db.PutEntity(key, &database.DataEntity{
		Data: value,
	})
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	db.addAof(utils.ToCmdLine2("getset", keys...))

	if !exists {
		return reply.MakeNullBulkReply()
	}
	return reply.MakeBulkReply(entity.Data.([]byte))
}

func execStrLen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeIntReply(int64(len(bytes)))
}

func init() {
	RegisterCommand("Get", execGet, 2)
	RegisterCommand("Set", execSet, 3)
	RegisterCommand("SetNx", execSetnx, 3)
	RegisterCommand("GetSet", execGetSet, 3)
	RegisterCommand("StrLen", execStrLen, 2)
}
