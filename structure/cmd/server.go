package main

import (
	"github.com/GrandeLai/JDawDB"
	"github.com/GrandeLai/JDawDB/structure"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

type DBServer struct {
	dbs    map[int]*structure.DataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	//打开redis数据结构服务
	dataStructure, err := structure.NewDataStructure(JDawDB.DefaultOptions)
	if err != nil {
		panic(err)
	}

	//初始化DBServer
	dbServer := &DBServer{
		dbs: make(map[int]*structure.DataStructure),
	}
	dbServer.dbs[0] = dataStructure
	//初始化一个redis服务器
	dbServer.server = redcon.NewServer(addr, execClientCommand, dbServer.accept, dbServer.close)
	dbServer.listen()

}

// 启动tcp服务，监听客户端的连接
func (srv *DBServer) listen() {
	log.Println("DBServer is running,ready to accept conn")
	_ = srv.server.ListenAndServe()
}

func (srv *DBServer) accept(conn redcon.Conn) bool {
	//监听到新的连接后初始化一个客户端
	cli := new(DBClient)
	srv.mu.Lock()
	defer srv.mu.Unlock()

	cli.server = srv
	cli.db = srv.dbs[0]
	conn.SetContext(cli)
	return true
}

func (srv *DBServer) close(conn redcon.Conn, err error) {
	for _, db := range srv.dbs {
		_ = db.Close()
	}
	_ = srv.server.Close()

}
