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

const LF = 0x0a // "\n"

func main() {
	err := run()
	if err != nil {
		if err != invalidArgument {
			log.Println(err)
			return
		}
		flag.PrintDefaults()
	}
}

var invalidArgument = errors.New("invalidArgument")

func run() error {
	isJson, delim, commandArgs, err := parseArgs()
	if err != nil {
		return err
	}

	m := &Map{m: make(map[string][]byte)}
	var parser Filterer
	if isJson {
		parser = NewJSONParser(m)
	} else {
		parser = NewParserByDelimiter(m, delim)
	}

	runners := Chain(
		os.Stdin,
		os.Stdout,
		os.Stderr,
		parser,
		NewCommandExecutor(commandArgs),
		NewResultMapper(m),
	)

	var (
		wg    sync.WaitGroup
		errch = make(chan error, len(runners))
	)
	for _, r := range runners {
		wg.Add(1)
		go func(r Runner) {
			defer wg.Done()
			errch <- r.Run()
		}(r)
	}
	wg.Wait()
	close(errch)

	var errs []error
	for err := range errch {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func parseArgs() (bool, []byte, []string, error) {
	delim := flag.String("d", " ", "delimiter(default is a space)")
	isJson := flag.Bool("json", false, "parse JSON object")

	if len(os.Args) < 3 {
		return false, nil, nil, invalidArgument
	}
	var commandArgs []string
	l := len(os.Args)
	for i := range os.Args {
		if os.Args[i] == "--" {
			if i == l-1 {
				return false, nil, nil, invalidArgument
			}
			commandArgs = os.Args[i+1:]
			os.Args = os.Args[:i]
			break
		}
	}
	if len(commandArgs) == 0 {
		return false, nil, nil, invalidArgument
	}

	flag.Parse()
	return *isJson, []byte(*delim), commandArgs, nil
}

type Map struct {
	m map[string][]byte
	sync.RWMutex
}

type FilterFunc func(io.Reader, io.Writer, io.Writer) error

func (f FilterFunc) Filter(in io.Reader, out, errout io.Writer) error { return f(in, out, errout) }

type Filterer interface {
	Filter(io.Reader, io.Writer, io.Writer) error
}

func NewParserByDelimiter(m *Map, delimiter []byte) FilterFunc {
	return func(in io.Reader, out, _ io.Writer) error {
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
			_, err := out.Write(append(k, LF))
			if err != nil {
				return err
			}
		}
		return scanner.Err()
	}
}

func NewJSONParser(m *Map) FilterFunc {
	return func(in io.Reader, out, _ io.Writer) error {
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
}

func NewCommandExecutor(commandArgs []string) FilterFunc {
	return func(in io.Reader, out, errout io.Writer) error {
		command := commandArgs[0]
		var args []string
		if len(commandArgs) > 1 {
			args = commandArgs[1:]
		}
		cmd := exec.Command(command, args...)
		cmd.Stdin = in
		cmd.Stdout = out
		cmd.Stderr = errout
		return cmd.Run()
	}
}

func NewResultMapper(m *Map) FilterFunc {
	return func(in io.Reader, out, _ io.Writer) error {
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
			_, err := out.Write(append(value, LF))
			if err != nil {
				return err
			}
		}
		return nil
	}
}

type Runner interface {
	Run() error
}

type RunnerFunc func() error

func (r RunnerFunc) Run() error {
	return r()
}

func NewFilterRunner(f Filterer, in io.Reader, out, errout io.Writer) RunnerFunc {
	return func() error {
		if closer, ok := out.(io.Closer); ok {
			defer closer.Close()
		}

		return f.Filter(in, out, errout)
	}
}

func Chain(in io.Reader, out, errout io.Writer, filterers ...Filterer) []Runner {
	var (
		runners  []Runner
		nextIn   io.Reader
		finalOut = out
	)

	for i, f := range filterers {
		if i == len(filterers)-1 {
			runners = append(runners, NewFilterRunner(f, in, finalOut, errout))
			break
		}
		nextIn, out = io.Pipe()
		runners = append(runners, NewFilterRunner(f, in, out, errout))
		in = nextIn
	}

	return runners
}
