package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"

	"golang.org/x/sync/errgroup"
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
	delim := flag.String("d", " ", "delimiter(default is a space)")
	isJson := flag.Bool("json", false, "parse JSON object")

	commandArgs, err := extractCommandArgs()
	if err != nil {
		return err
	}

	flag.Parse()

	eg, ctx := errgroup.WithContext(context.Background())
	var (
		m      InputMap
		parser Filterer
	)

	if *isJson {
		parser = NewJSONParser(ctx, &m)
	} else {
		parser = NewParserByDelimiter(ctx, &m, []byte(*delim))
	}

	runners := Chain(
		os.Stdin,
		os.Stdout,
		os.Stderr,
		parser,
		NewCommandExecutor(ctx, commandArgs),
		NewResultMapper(ctx, &m),
	)

	for _, r := range runners {
		eg.Go(r.Run)
	}
	return eg.Wait()
}

func extractCommandArgs() ([]string, error) {
	if len(os.Args) < 3 {
		return nil, invalidArgument
	}

	var (
		commandArgs []string
		l           = len(os.Args)
	)
	for i := range os.Args {
		if os.Args[i] == "--" {
			if i == l-1 {
				return nil, invalidArgument
			}
			commandArgs = os.Args[i+1:]
			os.Args = os.Args[:i]
			break
		}
	}

	if len(commandArgs) == 0 {
		return nil, invalidArgument
	}
	return commandArgs, nil
}

type Map[K comparable, V any] struct{ sync.Map }

func (m *Map[K, V]) Store(key K, value V) {
	m.Map.Store(key, value)
}

func (m *Map[K, V]) Load(key K) (V, bool) {
	value, ok := m.Map.Load(key)
	if !ok {
		var zeroValue V
		return zeroValue, false
	}
	return value.(V), true
}

func Append[K comparable, V any](m *Map[K, []V], key K, value V) {
	values, _ := m.Load(key)
	m.Store(key, append(values, value))
}

type InputMap = Map[string, [][]byte]

type FilterFunc func(io.Reader, io.Writer, io.Writer) error

func (f FilterFunc) Filter(in io.Reader, out, errout io.Writer) error { return f(in, out, errout) }

type Filterer interface {
	Filter(io.Reader, io.Writer, io.Writer) error
}

func NewParserByDelimiter(ctx context.Context, m *InputMap, delimiter []byte) FilterFunc {
	return func(in io.Reader, out, _ io.Writer) error {
		scanner := bufio.NewScanner(in)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// DO NOTHING
			}

			tsv := bytes.SplitN(scanner.Bytes(), delimiter, 2)
			if len(tsv) != 2 {
				return fmt.Errorf("input line is not tsv: %s", scanner.Text())
			}

			k, v := tsv[0], tsv[1]
			Append(m, string(k), v)
			_, err := out.Write(append(k, LF))
			if err != nil {
				return err
			}
		}
		return scanner.Err()
	}
}

func NewJSONParser(ctx context.Context, m *InputMap) FilterFunc {
	return func(in io.Reader, out, _ io.Writer) error {
		var mb map[string]json.RawMessage
		err := json.NewDecoder(in).Decode(&mb)
		if err != nil {
			return err
		}

		for k, v := range mb {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// DO NOTHING
			}

			Append[string, []byte](m, k, v)
			_, err := out.Write([]byte(k))
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func NewCommandExecutor(ctx context.Context, commandArgs []string) FilterFunc {
	return func(in io.Reader, out, errout io.Writer) error {
		command := commandArgs[0]
		var args []string
		if len(commandArgs) > 1 {
			args = commandArgs[1:]
		}
		cmd := exec.CommandContext(ctx, command, args...)
		cmd.Stdin = in
		cmd.Stdout = out
		cmd.Stderr = errout
		return cmd.Run()
	}
}

func NewResultMapper(ctx context.Context, m *InputMap) FilterFunc {
	return func(in io.Reader, out, _ io.Writer) error {
		scanner := bufio.NewScanner(in)
		var keys []string
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// DO NOTHING
			}

			keys = append(keys, scanner.Text())
		}
		err := scanner.Err()
		if err != nil {
			log.Println("error:", err)
			return err
		}
		set := make(map[string]struct{})
		for _, key := range keys {
			_, duplicated := set[key]
			if duplicated {
				continue
			}
			set[key] = struct{}{}

			values, ok := m.Load(key)
			if !ok {
				return fmt.Errorf("not found by key: %s, map: %#v", key, m)
			}
			for _, value := range values {
				_, err := out.Write(append(value, LF))
				if err != nil {
					return err
				}
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
