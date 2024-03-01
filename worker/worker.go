package worker

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/prodion23/haproxy-spoe-go/frame"
	"github.com/prodion23/haproxy-spoe-go/logger"
	"github.com/prodion23/haproxy-spoe-go/request"
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
	var f *frame.Frame

	defer w.close()
	timeStart := time.Now()

	defer func() {
		elapsed := time.Since(timeStart)
		if elapsed.Milliseconds() > 1000 {
			if f != nil {
				w.logger.Errorf("StreamID: %d run function total execution took %d ms", f.StreamID, elapsed.Milliseconds())
			} else {
				w.logger.Errorf("NO STREAMID - run function total execution took %d ms", elapsed.Milliseconds())
			}
		}

	}()

	buf := bufio.NewReader(w.conn)

	for {
		f = frame.AcquireFrame()
		readTimeStart := time.Now()

		if err := f.Read(buf); err != nil {
			elapsedRead := time.Since(readTimeStart)
			if elapsedRead.Milliseconds() > 1000 {
				w.logger.Errorf("StreamID: %d, read operation took %d ms", f.StreamID, elapsedRead.Milliseconds())
			}
			frame.ReleaseFrame(f)
			if err != io.EOF {
				return fmt.Errorf("error read frame: %v", err)
			}
			return nil
		}

		// Time taken to read the frame
		elapsedRead := time.Since(readTimeStart)
		if elapsedRead.Milliseconds() > 1000 {
			w.logger.Errorf("StreamID: %d, read operation completed in %d ms", f.StreamID, elapsedRead.Milliseconds())
		}

		switch f.Type {
		case frame.TypeHaproxyHello:
			helloTimeStart := time.Now()

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

			// Time taken for HaproxyHello processing
			elapsedHello := time.Since(helloTimeStart)
			if elapsedHello.Milliseconds() > 1000 {
				w.logger.Errorf("StreamID: %d HaproxyHello processing took %d ms", f.StreamID, elapsedHello.Milliseconds())
			}

		case frame.TypeHaproxyDisconnect:
			disconnectTimeStart := time.Now()

			if !w.ready {
				return fmt.Errorf("worker not ready, but got HaproxyDisconnect frame")
			}

			if err := w.sendAgentDisconnect(f, 0, "connection closed by server"); err != nil {
				return fmt.Errorf("error send AgentDisconnect frame: %v", err)
			}

			frame.ReleaseFrame(f)
			// Time taken for HaproxyDisconnect processing
			elapsedDisconnect := time.Since(disconnectTimeStart)
			if elapsedDisconnect.Milliseconds() > 1000 {
				w.logger.Errorf("StreamID: %d HaproxyDisconnect processing took %d ms", f.StreamID, elapsedDisconnect.Milliseconds())
			}
			return nil

		case frame.TypeNotify:
			notifyTimeStart := time.Now()

			if !w.ready {
				return fmt.Errorf("worker not ready, but got Notify frame")
			}

			w.processNotifyFrame(f)

			// Time taken to start processNotifyFrame go routine
			elapsedNotify := time.Since(notifyTimeStart)
			if elapsedNotify.Milliseconds() > 1000 {
				w.logger.Errorf("StreamID: %d Starting processNotifyFrame took %d ms", f.StreamID, elapsedNotify.Milliseconds())
			}

		default:
			w.logger.Errorf("unexpected frame type: %v", f.Type)
		}
	}
}
