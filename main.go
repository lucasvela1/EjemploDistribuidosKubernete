package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
)

var requestCount uint64

func handler(w http.ResponseWriter, r *http.Request) {
	count := atomic.AddUint64(&requestCount, 1)
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "desconocido"
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "========================================\n")
	fmt.Fprintf(w, "  DEMO KUBERNETES - SISTEMAS DISTRIBUIDOS\n")
	fmt.Fprintf(w, "========================================\n")
	fmt.Fprintf(w, "Pod (Hostname): %s\n", hostname)
	fmt.Fprintf(w, "Peticiones atendidas por este pod: %d\n", count)
	fmt.Fprintf(w, "Ruta consultada: %s\n", r.URL.Path)
}

func main() {
	http.HandleFunc("/", handler)
	port := ":8080"
	fmt.Printf("Servidor iniciado en el puerto %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Error al iniciar el servidor: %s\n", err)
	}
}
