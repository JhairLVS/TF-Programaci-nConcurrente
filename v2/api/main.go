package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

// Estructuras de datos
type Result struct {
	ProductID string  `json:"product_id"`
	Stars     float64 `json:"stars"`
	Category  string  `json:"category"`
}

// Configuración global
var (
	config = struct {
		Categories []string
		MaxResults int
	}{}
	recommendations []Result
	connections     []*websocket.Conn
	connLock        sync.Mutex
	upgrader        = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
)

func main() {
	router := mux.NewRouter()

	// Endpoints REST
	router.HandleFunc("/api/config", postConfig).Methods("POST")
	router.HandleFunc("/api/recommendations", getRecommendations).Methods("GET")
	router.HandleFunc("/ws", handleWebSocket)

	// Iniciar servidor HTTP
	fmt.Println("Servidor API escuchando en http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}

// POST /api/config - Recibe configuración y conecta con el cluster
func postConfig(w http.ResponseWriter, r *http.Request) {
	var newConfig struct {
		Categories []string `json:"categories"`
		MaxResults int      `json:"max_results"`
	}
	if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}

	config.Categories = newConfig.Categories
	config.MaxResults = newConfig.MaxResults

	// Conectar con el cluster
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		http.Error(w, "Error al conectar con el servidor del cluster", http.StatusInternalServerError)
		log.Println("Error al conectar con el servidor del cluster:", err)
		return
	}
	defer conn.Close()

	// Enviar configuración y recibir resultados
	configJSON, _ := json.Marshal(newConfig)
	fmt.Fprintf(conn, "%s\n", configJSON)

	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		http.Error(w, "Error al recibir datos del cluster", http.StatusInternalServerError)
		log.Println("Error al recibir datos del cluster:", err)
		return
	}

	// Actualizar recomendaciones
	json.Unmarshal([]byte(strings.TrimSpace(response)), &recommendations)

	// Notificar a los clientes WebSocket
	notifyClients(recommendations)

	// Respuesta
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// GET /api/recommendations - Devuelve recomendaciones
func getRecommendations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(recommendations)
}

// WebSocket /ws - Maneja conexiones WebSocket
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error al establecer conexión WebSocket:", err)
		return
	}
	connLock.Lock()
	connections = append(connections, conn)
	connLock.Unlock()
}

// Notifica a los clientes WebSocket
func notifyClients(results []Result) {
	connLock.Lock()
	defer connLock.Unlock()

	for _, conn := range connections {
		if err := conn.WriteJSON(results); err != nil {
			log.Println("Error enviando datos por WebSocket:", err)
			conn.Close()
		}
	}
}
