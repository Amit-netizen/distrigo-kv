package server

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"

	"github.com/tidwall/resp"
)

// -----------------------------------------------------------------------
// Command types
// -----------------------------------------------------------------------

type command interface{ isCmd() }

type clientCmd struct{ value string }
type helloCmd struct{ value string }
type getCmd struct{ key []byte }
type setCmd struct {
	key []byte
	val []byte
	ttl int64
}
type delCmd struct{ key []byte }

func (clientCmd) isCmd() {}
func (helloCmd) isCmd()  {}
func (getCmd) isCmd()    {}
func (setCmd) isCmd()    {}
func (delCmd) isCmd()    {}

// -----------------------------------------------------------------------
// Peer
// -----------------------------------------------------------------------

type peer struct {
	conn  net.Conn
	msgCh chan message
	delCh chan *peer
}

func newPeer(conn net.Conn, msgCh chan message, delCh chan *peer) *peer {
	return &peer{conn: conn, msgCh: msgCh, delCh: delCh}
}

func (p *peer) send(b []byte) (int, error) {
	return p.conn.Write(b)
}

// readLoop parses RESP frames from the connection and forwards typed commands
// onto msgCh.  It runs until the connection is closed.
func (p *peer) readLoop() error {
	rd := resp.NewReader(p.conn)
	for {
		v, _, err := rd.ReadValue()
		if err == io.EOF {
			p.delCh <- p
			break
		}
		if err != nil {
			p.delCh <- p
			return err
		}

		if v.Type() != resp.Array || len(v.Array()) == 0 {
			continue
		}

		raw := strings.ToLower(v.Array()[0].String())
		arr := v.Array()

		var cmd command
		switch raw {
		case "client":
			if len(arr) > 1 {
				cmd = clientCmd{value: arr[1].String()}
			}
		case "hello":
			val := ""
			if len(arr) > 1 {
				val = arr[1].String()
			}
			cmd = helloCmd{value: val}
		case "get":
			if len(arr) < 2 {
				slog.Warn("GET: missing key")
				continue
			}
			cmd = getCmd{key: arr[1].Bytes()}
		case "set":
			if len(arr) < 3 {
				slog.Warn("SET: missing key or value")
				continue
			}
			s := setCmd{
				key: arr[1].Bytes(),
				val: arr[2].Bytes(),
			}
			if len(arr) > 3 {
				s.ttl = int64(arr[3].Integer())
			}
			cmd = s
		case "del":
			if len(arr) < 2 {
				slog.Warn("DEL: missing key")
				continue
			}
			cmd = delCmd{key: arr[1].Bytes()}
		default:
			slog.Debug("unhandled command", "cmd", raw)
			_, _ = p.send([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", raw)))
			continue
		}

		p.msgCh <- message{cmd: cmd, peer: p}
	}
	return nil
}
