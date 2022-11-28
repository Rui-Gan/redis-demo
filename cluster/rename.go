package cluster

import (
	"redis/interface/resp"
	"redis/resp/reply"
)

func Rename(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	if len(cmdArgs) != 3 {
		return reply.MakeErrReply("ERR wrong number args")
	}
	src := string(cmdArgs[1])
	dst := string(cmdArgs[2])

	srcPeer := cluster.peerPicker.PickNode(src)
	dstPeer := cluster.peerPicker.PickNode(dst)
	if srcPeer != dstPeer {
		return reply.MakeErrReply("ERR rename must within one peer")
	}
	return cluster.relay(srcPeer, c, cmdArgs)
}
