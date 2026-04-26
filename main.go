// Брокер очередей сообщений — FIFO очередь поверх HTTP.
//
// Запуск: queue-broker <port>
//
//	PUT /queue?v=message  — положить сообщение в очередь
//	GET /queue            — забрать сообщение (FIFO)
//	GET /queue?timeout=N  — забрать с ожиданием N секунд
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// queue представляет FIFO очередь сообщений.
// messages — буфер сообщений; waiters — каналы ожидающих читателей.
type queue struct {
	mu       sync.Mutex
	messages []string
	waiters  []chan string
}

// broker управляет множеством именованных очередей.
type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

// get возвращает очередь по имени, создавая новую при необходимости.
func (b *broker) get(name string) *queue {
	b.mu.Lock()
	q, ok := b.queues[name]
	if !ok {
		q = &queue{}
		b.queues[name] = q
	}
	b.mu.Unlock()
	return q
}

// put добавляет сообщение в очередь. При наличии ожидающих читателей
// сообщение доставляется первому из них (FIFO), иначе буферизуется.
func (q *queue) put(msg string) {
	q.mu.Lock()
	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w <- msg
	} else {
		q.messages = append(q.messages, msg)
	}
	q.mu.Unlock()
}

// cleanup удаляет канал w из списка ожидающих читателей.
// Если в канале уже есть сообщение (гонка с put), оно сохраняется в буфер.
func (q *queue) cleanup(w chan string) {
	q.mu.Lock()
	for i, ch := range q.waiters {
		if ch == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			break
		}
	}
	select {
	case msg := <-w:
		q.messages = append(q.messages, msg)
	default:
	}
	q.mu.Unlock()
}

// get извлекает сообщение из очереди (FIFO) или ожидает его появления.
// При timeout == 0 и пустой очереди сразу возвращает false.
// При ненулевом timeout ожидает сообщение, отмену контекста или истечение таймаута.
func (q *queue) get(ctx context.Context, timeout time.Duration) (string, bool) {
	q.mu.Lock()
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		q.mu.Unlock()
		return msg, true
	}
	if timeout == 0 {
		q.mu.Unlock()
		return "", false
	}
	w := make(chan string, 1)
	q.waiters = append(q.waiters, w)
	q.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-w:
		return msg, true
	case <-ctx.Done():
		q.cleanup(w)
		return "", false
	case <-timer.C:
		q.cleanup(w)
		return "", false
	}
}

// main —
// аргумент командной строки — номер порта.
// Регистрирует единый обработчик для всех путей,
// запускает HTTP-сервер без таймаутов (необходимо для long-poll).
func main() {
	if len(os.Args) != 2 {
		os.Exit(1)
	}
	b := &broker{queues: make(map[string]*queue)}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[1:]
		if name == "" {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodPut:
			if !r.URL.Query().Has("v") {
				http.Error(w, "", http.StatusBadRequest)
				return
			}
			msg := r.URL.Query().Get("v")
			b.get(name).put(msg)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			var timeout time.Duration
			if t := r.URL.Query().Get("timeout"); t != "" {
				s, err := strconv.Atoi(t)
				if err != nil || s < 0 {
					http.Error(w, "", http.StatusBadRequest)
					return
				}
				timeout = time.Duration(s) * time.Second
			}
			msg, ok := b.get(name).get(r.Context(), timeout)
			if ok {
				fmt.Fprint(w, msg)
			} else {
				http.Error(w, "", http.StatusNotFound)
			}
		default:
			http.Error(w, "", http.StatusMethodNotAllowed)
		}
	})
	if err := http.ListenAndServe(":"+os.Args[1], mux); err != nil {
		os.Exit(1)
	}
}
