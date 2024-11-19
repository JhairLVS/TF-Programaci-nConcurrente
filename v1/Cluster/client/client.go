package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strings"
)

type Result struct {
	ProductID string  `json:"product_id"`
	Stars     float64 `json:"stars"`
	Category  string  `json:"category"`
}

func main() {
	// Leer el puerto desde los argumentos
	if len(os.Args) < 2 {
		fmt.Println("Por favor, proporciona un puerto como argumento")
		return
	}
	port := ":" + os.Args[1]

	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("Error al iniciar el cliente:", err)
		return
	}
	defer listener.Close()

	fmt.Printf("Cliente escuchando en el puerto %s\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error al aceptar conexión:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Println("Conexión establecida con el maestro.")
	// Recibir tarea
	task, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("Error al recibir tarea:", err)
		return
	}
	var partition map[string]map[string]float64
	if err := json.Unmarshal([]byte(strings.TrimSpace(task)), &partition); err != nil {
		log.Printf("Error decodificando datos: %v", err)
		return
	}
	fmt.Println("Calculando similitudes entre productos...")
	similarities := calculateCosineSimilarities(partition)

	fmt.Println("Escalando las similitudes...")
	scaledSimilarities := scaleSimilitudes(similarities)

	fmt.Println("Calculando las puntuaciones predichas...")
	results := predictRatings(scaledSimilarities, partition)

	// Serializar los resultados a JSON
	response, _ := json.Marshal(results)
	fmt.Printf("Enviando resultados al maestro: %d resultados\n", len(results))

	// Enviar resultado al servidor
	fmt.Fprintf(conn, "%s\n", response) // Enviar JSON como cadena
}

func scaleSimilitudes(similarities map[string]map[string]float64) map[string]map[string]float64 {
	var minSim, maxSim float64
	minSim = math.Inf(1)
	maxSim = math.Inf(-1)

	for _, relatedItems := range similarities {
		for _, sim := range relatedItems {
			if sim < minSim {
				minSim = sim
			}
			if sim > maxSim {
				maxSim = sim
			}
		}
	}

	scaledSimilarities := make(map[string]map[string]float64)
	for i, relatedItems := range similarities {
		scaledSimilarities[i] = make(map[string]float64)
		for j, similarity := range relatedItems {
			scaledSimilarity := (similarity - minSim) / (maxSim - minSim)
			scaledSimilarities[i][j] = scaledSimilarity
		}
	}

	return scaledSimilarities
}

func calculateCosineSimilarities(partition map[string]map[string]float64) map[string]map[string]float64 {
	similarities := make(map[string]map[string]float64)

	for _, items := range partition {
		for i, ratingI := range items {
			if _, exists := similarities[i]; !exists {
				similarities[i] = make(map[string]float64)
			}
			for j, ratingJ := range items {
				if i == j {
					continue
				}
				similarities[i][j] += ratingI * ratingJ
			}
		}
	}

	return similarities
}

func predictRatings(similarities map[string]map[string]float64, partition map[string]map[string]float64) []Result {
	results := make(map[string]Result)

	for _, items := range partition {
		for productID, rating := range items {
			for similarProduct, similarity := range similarities[productID] {
				predictedScore := similarity * rating
				results[similarProduct] = Result{
					ProductID: similarProduct,
					Stars:     predictedScore,
					Category:  "unknown",
				}
			}
		}
	}

	finalResults := []Result{}
	for _, res := range results {
		finalResults = append(finalResults, res)
	}
	return finalResults
}
