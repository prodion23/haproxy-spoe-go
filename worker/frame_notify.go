package worker

import (
	"bytes"
	"fmt"
	"time"

	"github.com/prodion23/haproxy-spoe-go/frame"
	"github.com/prodion23/haproxy-spoe-go/request"
)

func (w *worker) processNotifyFrame(f *frame.Frame) {
	timeStart := time.Now()
	defer frame.ReleaseFrame(f)

	req := request.AcquireRequest()
	defer request.ReleaseRequest(req)

	req.StreamID = f.StreamID
	req.FrameID = f.FrameID
	req.EngineID = w.engineID
	req.Messages = f.Messages

	w.handler(req)

	ackFrame := frame.AcquireFrame()
	defer frame.ReleaseFrame(ackFrame)

	ackFrame.Type = frame.TypeAgentAck
	ackFrame.StreamID = f.StreamID
	ackFrame.FrameID = f.FrameID
	ackFrame.Actions = req.Actions

	err := w.writeFrame(ackFrame)
	elapsed := time.Since(timeStart)
	if err != nil {
		w.logger.Errorf("StreamID: %d total_ms: %d, err: ack frame write failed: %v", ackFrame.StreamID, elapsed.Milliseconds(), err)
	}
}

func (w *worker) writeFrame(f *frame.Frame) error {
	buf := bytes.NewBuffer(make([]byte, 0))
	n, err := f.Encode(buf)
	if err != nil {
		return fmt.Errorf("cannot marshal frame: %w", err)
	}

	n, err = w.conn.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("cannot write frame to connection: %w", err)
	}

	if n != buf.Len() {
		return fmt.Errorf("wrote wrong number of bytes count %d, expect %d", n, buf.Len())
	}

	return nil
}
