package server

import (
	"errors"
	"strconv"

	"github.com/colonyos/colonies/pkg/core"
)

func (g *coloniesGraphs) findSuccessfulProcessGraphs(colonyName string, count int) ([]*core.ProcessGraph, error) {
	cmd := &command{threaded: true, processGraphsReplyChan: make(chan []*core.ProcessGraph),
		errorChan: make(chan error, 1),
		handler: func(cmd *command) {
			if count > MAX_COUNT {
				cmd.errorChan <- errors.New("Count is larger than MaxCount limit <" + strconv.Itoa(MAX_COUNT) + ">")
				return
			}
			graphs, err := g.db.FindSuccessfulProcessGraphs(colonyName, count)
			if err != nil {
				cmd.errorChan <- err
				return
			}
			for _, graph := range graphs {
				err = g.updateProcessGraph(graph)
				if err != nil {
					cmd.errorChan <- err
					return
				}
			}
			cmd.processGraphsReplyChan <- graphs
		}}

	g.cmdQueue <- cmd
	var graphs []*core.ProcessGraph
	select {
	case err := <-cmd.errorChan:
		return graphs, err
	case graphs := <-cmd.processGraphsReplyChan:
		return graphs, nil
	}
}

func (g *coloniesGraphs) findFailedProcessGraphs(colonyName string, count int) ([]*core.ProcessGraph, error) {
	cmd := &command{threaded: true, processGraphsReplyChan: make(chan []*core.ProcessGraph),
		errorChan: make(chan error, 1),
		handler: func(cmd *command) {
			if count > MAX_COUNT {
				cmd.errorChan <- errors.New("Count is larger than MaxCount limit <" + strconv.Itoa(MAX_COUNT) + ">")
				return
			}
			graphs, err := g.db.FindFailedProcessGraphs(colonyName, count)
			if err != nil {
				cmd.errorChan <- err
				return
			}
			for _, graph := range graphs {
				err = g.updateProcessGraph(graph)
				if err != nil {
					cmd.errorChan <- err
					return
				}
			}
			cmd.processGraphsReplyChan <- graphs
		}}

	g.cmdQueue <- cmd
	var graphs []*core.ProcessGraph
	select {
	case err := <-cmd.errorChan:
		return graphs, err
	case graphs := <-cmd.processGraphsReplyChan:
		return graphs, nil
	}
}

func (g *coloniesGraphs) findWaitingProcessGraphs(colonyName string, count int) ([]*core.ProcessGraph, error) {
	cmd := &command{threaded: true, processGraphsReplyChan: make(chan []*core.ProcessGraph),
		errorChan: make(chan error, 1),
		handler: func(cmd *command) {
			if count > MAX_COUNT {
				cmd.errorChan <- errors.New("Count is larger than MaxCount limit <" + strconv.Itoa(MAX_COUNT) + ">")
				return
			}
			graphs, err := g.db.FindWaitingProcessGraphs(colonyName, count)
			if err != nil {
				cmd.errorChan <- err
				return
			}
			for _, graph := range graphs {
				err = g.updateProcessGraph(graph)
				if err != nil {
					cmd.errorChan <- err
					return
				}
			}

			cmd.processGraphsReplyChan <- graphs
		}}

	g.cmdQueue <- cmd
	var graphs []*core.ProcessGraph
	select {
	case err := <-cmd.errorChan:
		return graphs, err
	case graphs := <-cmd.processGraphsReplyChan:
		return graphs, nil
	}
}

func (g *coloniesGraphs) findRunningProcessGraphs(colonyName string, count int) ([]*core.ProcessGraph, error) {
	cmd := &command{threaded: true, processGraphsReplyChan: make(chan []*core.ProcessGraph),
		errorChan: make(chan error, 1),
		handler: func(cmd *command) {
			if count > MAX_COUNT {
				cmd.errorChan <- errors.New("Count is larger than MaxCount limit <" + strconv.Itoa(MAX_COUNT) + ">")
				return
			}
			graphs, err := g.db.FindRunningProcessGraphs(colonyName, count)
			if err != nil {
				cmd.errorChan <- err
				return
			}
			for _, graph := range graphs {
				err = g.updateProcessGraph(graph)
				if err != nil {
					cmd.errorChan <- err
					return
				}
			}
			cmd.processGraphsReplyChan <- graphs
		}}

	g.cmdQueue <- cmd
	var graphs []*core.ProcessGraph
	select {
	case err := <-cmd.errorChan:
		return graphs, err
	case graphs := <-cmd.processGraphsReplyChan:
		return graphs, nil
	}
}

func (g *coloniesGraphs) getProcessGraphByID(processGraphID string) (*core.ProcessGraph, error) {
	cmd := &command{threaded: true, processGraphReplyChan: make(chan *core.ProcessGraph, 1),
		errorChan: make(chan error, 1),
		handler: func(cmd *command) {
			graph, err := g.db.GetProcessGraphByID(processGraphID)
			if err != nil {
				cmd.errorChan <- err
				return
			}
			if graph == nil {
				cmd.processGraphReplyChan <- nil
				return
			}
			err = g.updateProcessGraph(graph)
			if err != nil {
				cmd.errorChan <- err
				return
			}

			cmd.processGraphReplyChan <- graph
		}}

	g.cmdQueue <- cmd
	select {
	case err := <-cmd.errorChan:
		return nil, err
	case graph := <-cmd.processGraphReplyChan:
		return graph, nil
	}
}

func (g *coloniesGraphs) updateProcessGraph(graph *core.ProcessGraph) error {
	graph.SetStorage(g.db)
	return graph.UpdateProcessIDs()
}
