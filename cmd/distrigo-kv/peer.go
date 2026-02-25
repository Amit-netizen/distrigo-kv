package main

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/tidwall/resp"
)

type Peer struct {
	conn  net.Conn
	msgCh chan Message
	delCh chan *Peer
}

func (p *Peer) Send(msg []byte) (int, error) {
	return p.conn.Write(msg)
}

func NewPeer(conn net.Conn, msgCh chan Message, delCh chan *Peer) *Peer {
	return &Peer{
		conn:  conn,
		msgCh: msgCh,
		delCh: delCh,
	}
}

func (p *Peer) readLoop() error {

	rd := resp.NewReader(p.conn)

	for {

		v, _, err := rd.ReadValue()

		if err == io.EOF {
			p.delCh <- p
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		var cmd Command

		if v.Type() == resp.Array {

			rawCMD := v.Array()[0].String()

			switch rawCMD {

			case CommandClient:

				cmd = ClientCommand{
					value: v.Array()[1].String(),
				}

			case CommandGET:

				cmd = GetCommand{
					key: v.Array()[1].Bytes(),
				}

			case CommandSET:

				// declare setCmd properly
				setCmd := SetCommand{
					key: v.Array()[1].Bytes(),
					val: v.Array()[2].Bytes(),
					ttl: 0,
				}

				if len(v.Array()) > 3 {
					setCmd.ttl = int64(v.Array()[3].Integer())
				}

				cmd = setCmd

			case CommandHELLO:

				cmd = HelloCommand{
					value: v.Array()[1].String(),
				}

			default:

				fmt.Println("Unhandled command:", rawCMD)
			}

			p.msgCh <- Message{
				cmd:  cmd,
				peer: p,
			}
		}
	}

	return nil
}