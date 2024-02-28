package worker

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/negasus/haproxy-spoe-go/frame"
	"github.com/negasus/haproxy-spoe-go/logger"
	"github.com/negasus/haproxy-spoe-go/request"
)

const (
	capabilities = "pipelining,async"
)

// Handle listen connection and process frames
func Handle(conn net.Conn, handler func(*request.Request), logger logger.Logger) {
	w := &worker{
		conn:    conn,
		handler: handler,
		logger:  logger,
	}

	if err := w.run(); err != nil {
		logger.Errorf("handle worker: %v", err)
	}
}

type worker struct {
	conn     net.Conn
	ready    bool
	engineID string
	handler  func(*request.Request)

	logger logger.Logger
}

func (w *worker) close() {
	if err := w.conn.Close(); err != nil {
		w.logger.Errorf("close connection: %v", err)
	}
}

func (w *worker) run() error {

	defer w.close()
	timeStart := time.Now()

	defer func() {
		elapsed := time.Since(timeStart)
		w.logger.Errorf("run function took %d ms\n", elapsed.Milliseconds())
	}()

	var f *frame.Frame

	buf := bufio.NewReader(w.conn)

	for {
		f = frame.AcquireFrame()
		readTimeStart := time.Now()
		if err := f.Read(buf); err != nil {
			frame.ReleaseFrame(f)
			elapsedRead := time.Since(readTimeStart)
			w.logger.Errorf("FRAME: %d read frame took %d ms\n", f.FrameID, elapsedRead.Milliseconds())
			if err != io.EOF {
				return fmt.Errorf("error read frame: %v", err)
			}
			return nil
		}
		elapsedRead := time.Since(readTimeStart)
		w.logger.Errorf("FRAME: %d read frame took %d ms\n", f.FrameID, elapsedRead.Milliseconds())
		switch f.Type {
		case frame.TypeHaproxyHello:

			if w.ready {
				return fmt.Errorf("worker already ready, but got HaproxyHello frame")
			}

			if err := w.sendAgentHello(f); err != nil {
				frame.ReleaseFrame(f)
				return fmt.Errorf("error send AgentHello frame: %v", err)
			}

			if f.Healthcheck {
				frame.ReleaseFrame(f)
				return nil
			}

			w.engineID = f.EngineID

			w.ready = true
			continue

		case frame.TypeHaproxyDisconnect:
			if !w.ready {
				return fmt.Errorf("worker not ready, but got HaproxyDisconnect frame")
			}

			if err := w.sendAgentDisconnect(f, 0, "connection closed by server"); err != nil {
				return fmt.Errorf("error send AgentDisconnect frame: %v", err)
			}
			frame.ReleaseFrame(f)
			return nil

		case frame.TypeNotify:
			if !w.ready {
				return fmt.Errorf("worker not ready, but got Notify frame")
			}

			go w.processNotifyFrame(f)

		default:
			w.logger.Errorf("unexpected frame type: %v", f.Type)
		}
	}
}
