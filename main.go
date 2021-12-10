package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"log"
)

func main() {
	err := run()
	if err != nil {
		if err != invalidArgument {
			log.Println(err)
		}
		flag.PrintDefaults()
	}
}

var invalidArgument = errors.New("invalidArgument")

func run() error {
	delim := flag.String("d", " ", "delimiter(default is a space)")
	isJson := flag.Bool("json", false, "parse JSON object")

	if len(os.Args) < 3 {
		return invalidArgument
	}
	var commandArgs []string
	l := len(os.Args)
	for i := range os.Args {
		if os.Args[i] == "--" {
			if i == l-1 {
				return invalidArgument
			}
			commandArgs = os.Args[i+1:]
			os.Args = os.Args[:i]
			break
		}
	}
	if len(commandArgs) == 0 {
		return invalidArgument
	}

	flag.Parse()

	pr1, pw1 := io.Pipe()
	pr2, pw2 := io.Pipe()
	m := &Map{m: make(map[string][]byte)}

	var w1 *Worker
	if *isJson {
		w1 = NewWorker("parseJSON", func() (err error) {
			defer func() {
				closeErr := pw1.Close()
				if err == nil && closeErr != nil {
					err = closeErr
				}
			}()
			return parseJSON(m, os.Stdin, pw1)
		})
	} else {
		w1 = NewWorker("parseByDelimiter", func() (err error) {
			defer func() {
				closeErr := pw1.Close()
				if err == nil && closeErr != nil {
					err = closeErr
				}
			}()
			return parseByDelimiter([]byte(*delim), m, os.Stdin, pw1)
		})
	}
	go w1.Run()

	w2 := NewWorker("execCommand", func() (err error) {
		defer func() {
			closeErr := pw2.Close()
			if err == nil && closeErr != nil {
				err = closeErr
			}
		}()
		return execCommand(commandArgs, pr1, pw2, os.Stderr)
	})
	go w2.Run()

	w3 := NewWorker("filterMap", func() error {
		return filterMap(m, pr2, os.Stdout)
	})
	go w3.Run()

LOOP:
	for {
		select {
		case _, _ = <-w1.Done:
			if err := w1.Err; err != nil {
				return err
			}
			if w1.IsDone() && w2.IsDone() && w3.IsDone() {
				break LOOP
			}
		case _, _ = <-w2.Done:
			if err := w2.Err; err != nil {
				return err
			}
			if w1.IsDone() && w2.IsDone() && w3.IsDone() {
				break LOOP
			}
		case _, _ = <-w3.Done:
			if err := w3.Err; err != nil {
				return err
			}
			if w1.IsDone() && w2.IsDone() && w3.IsDone() {
				break LOOP
			}
		}
	}

	return nil
}

type Map struct {
	m map[string][]byte
	sync.RWMutex
}

func parseByDelimiter(delimiter []byte, m *Map, in io.Reader, out io.Writer) error {
	m.Lock()
	defer m.Unlock()
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		tsv := bytes.SplitN(scanner.Bytes(), delimiter, 2)
		if len(tsv) != 2 {
			return fmt.Errorf("input line is not tsv: %s", scanner.Text())
		}

		k, v := tsv[0], tsv[1]
		m.m[string(k)] = v
		_, err := out.Write(k)
		if err != nil {
			return err
		}
	}
	return scanner.Err()
}

func parseJSON(m *Map, in io.Reader, out io.Writer) error {
	var mb map[string]json.RawMessage
	err := json.NewDecoder(in).Decode(&mb)
	if err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()
	for k, v := range mb {
		m.m[k] = v
		_, err := out.Write([]byte(k))
		if err != nil {
			return err
		}
	}
	return nil
}

func execCommand(commandArgs []string, in io.Reader, out, errio io.Writer) error {
	command := commandArgs[0]
	var args []string
	if len(commandArgs) > 1 {
		args = commandArgs[1:]
	}
	cmd := exec.Command(command, args...)
	cmd.Stdin = in
	cmd.Stdout = out
	cmd.Stderr = errio
	return cmd.Run()
}

func filterMap(m *Map, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	var keys []string
	for scanner.Scan() {
		keys = append(keys, scanner.Text())
	}
	err := scanner.Err()
	if err != nil {
		log.Println("error:", err)
		return err
	}
	m.RLock()
	defer m.RUnlock()
	for _, key := range keys {
		value, ok := m.m[key]
		if !ok {
			return fmt.Errorf("not found by key: %s, map: %#v", key, m.m)
		}
		_, err := out.Write(append(value, []byte("\n")...))
		if err != nil {
			return err
		}
	}
	return nil
}

type Worker struct {
	Name string
	sync.RWMutex
	isDone bool
	Done   chan struct{}
	Err    error
	f      func() error
}

func (w *Worker) Run() {
	err := w.f()
	w.Lock()
	w.isDone = true
	w.Err = err
	w.Unlock()
	w.Done <- struct{}{}
}

func (w *Worker) IsDone() bool {
	w.RLock()
	defer w.RUnlock()
	return w.isDone
}

func NewWorker(name string, f func() error) *Worker {
	return &Worker{Name: name, Done: make(chan struct{}), Err: nil, f: f}
}
