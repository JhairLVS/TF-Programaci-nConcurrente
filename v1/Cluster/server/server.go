package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Estructuras definidas
type Product struct {
	ReviewerID      string  `json:"reviewer_id"`
	ProductID       string  `json:"product_id"`
	Stars           float64 `json:"stars"`
	ProductCategory string  `json:"product_category"`
}

type Result struct {
	ProductID string  `json:"product_id"`
	Stars     float64 `json:"stars"`
	Category  string  `json:"category"`
}

func main() {
	clients := []string{
		"client1:9001",
		"client2:9002",
		"client3:9003",
	}

	// Leer y parsear el CSV a una lista de productos
	products, err := readCSV("amazon_reviews_cleaned.csv")
	if err != nil {
		fmt.Println("Error al leer el archivo CSV:", err)
		return
	}

	// Filtrar productos por categorías seleccionadas enviadas de api
	selectedCategories := []string{"electronics", "books"}
	filteredProducts := filterByCategories(products, selectedCategories)

	// Construir matriz usuario-ítem
	userItemMatrix := buildUserItemMatrix(filteredProducts)
	fmt.Println("Matriz usuario-ítem construida!")

	// Particionar la matriz usuario-ítem
	partitions := partitionUserItemMatrix(userItemMatrix, len(clients))

	var wg sync.WaitGroup
	productScores := make(map[string][]float64)
	mu := &sync.Mutex{} // Mutex para acceso concurrente al mapa

	// Verificar la disponibilidad de los esclavos
	availableClients := checkClientsAvailability(clients)

	// Enviar datos a los nodos disponibles
	for i, clientAddress := range availableClients {
		wg.Add(1)
		go func(clientAddress string, partition map[string]map[string]float64) {
			defer wg.Done()
			results := processUserItemPartition(clientAddress, partition)
			mu.Lock()
			for _, res := range results {
				productScores[res.ProductID] = append(productScores[res.ProductID], res.Stars)
			}
			mu.Unlock()
		}(clientAddress, partitions[i])
	}

	wg.Wait()

	// Calcular el promedio de las puntuaciones predichas
	finalResults := calculateAverage(productScores)
	// Ordenar los resultados por puntuación descendente
	sort.Slice(finalResults, func(i, j int) bool {
		return finalResults[i].Stars > finalResults[j].Stars
	})

	// lo pasa api
	cont := 5

	// Mostrar solo los 5 mejores resultados
	fmt.Println("Top 5 mejores recomendaciones:")
	for i := 0; i < cont && i < len(finalResults); i++ {
		res := finalResults[i]
		fmt.Printf("ProductID: %s, Stars: %.1f, Category: %s\n", res.ProductID, res.Stars, res.Category)
	}
}

// Leer CSV y parsear a una lista de productos
func readCSV(filePath string) ([]Product, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var products []Product
	for _, record := range records[1:] { // Ignorar la primera fila (cabeceras)
		stars, _ := strconv.ParseFloat(record[2], 64)
		product := Product{
			ReviewerID:      record[0],
			ProductID:       record[1],
			Stars:           stars,
			ProductCategory: record[3],
		}
		products = append(products, product)
	}
	return products, nil
}

// Filtrar productos por categorías seleccionadas
func filterByCategories(products []Product, categories []string) []Product {
	var filtered []Product
	categorySet := make(map[string]bool)
	for _, category := range categories {
		categorySet[category] = true
	}

	for _, product := range products {
		if categorySet[product.ProductCategory] {
			filtered = append(filtered, product)
		}
	}
	return filtered
}

// Construir matriz usuario-ítem
func buildUserItemMatrix(products []Product) map[string]map[string]float64 {
	matrix := make(map[string]map[string]float64)
	for _, product := range products {
		if _, exists := matrix[product.ReviewerID]; !exists {
			matrix[product.ReviewerID] = make(map[string]float64)
		}
		matrix[product.ReviewerID][product.ProductID] = product.Stars
	}
	return matrix
}

// Particionar la matriz usuario-ítem
func partitionUserItemMatrix(matrix map[string]map[string]float64, numPartitions int) []map[string]map[string]float64 {
	partitions := make([]map[string]map[string]float64, numPartitions)
	for i := range partitions {
		partitions[i] = make(map[string]map[string]float64)
	}

	i := 0
	for user, items := range matrix {
		partitions[i][user] = items
		i = (i + 1) % numPartitions
	}
	return partitions
}

// Procesar una partición de la matriz usuario-ítem
func processUserItemPartition(clientAddress string, partition map[string]map[string]float64) []Result {
	conn, err := net.Dial("tcp", clientAddress)
	if err != nil {
		fmt.Printf("Error al conectar con el cliente %s: %v\n", clientAddress, err)
		return nil
	}
	defer conn.Close()

	// Enviar partición al cliente en formato JSON
	partitionJSON, _ := json.Marshal(partition)
	fmt.Fprintf(conn, "%s\n", partitionJSON)

	// Recibir resultados del cliente
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Printf("Error al recibir respuesta del cliente %s: %v\n", clientAddress, err)
		return nil
	}

	// Deserializar resultados
	var results []Result
	if err := json.Unmarshal([]byte(strings.TrimSpace(response)), &results); err != nil {
		fmt.Printf("Error decodificando datos del cliente %s: %v\n", clientAddress, err)
		return nil
	}

	// Imprimir los resultados recibidos de este cliente
	fmt.Printf("Resultados recibidos de %s: %d resultados\n", clientAddress, len(results))

	return results
}

// Calcular el promedio de las puntuaciones predichas
func calculateAverage(productScores map[string][]float64) []Result {
	var finalResults []Result

	for productID, scores := range productScores {
		var sum float64
		for _, score := range scores {
			sum += score
		}
		average := sum / float64(len(scores))

		finalResults = append(finalResults, Result{
			ProductID: productID,
			Stars:     average,
			Category:  "unknown", // Puedes asignar una categoría si lo deseas
		})
	}
	fmt.Println("Resultados promediados finales:%d resultados\n", len(finalResults))
	return finalResults
}

// Verificar disponibilidad de clientes
func checkClientsAvailability(clients []string) []string {
	var availableClients []string
	for _, client := range clients {
		conn, err := net.DialTimeout("tcp", client, 2*time.Second)
		if err == nil {
			availableClients = append(availableClients, client)
			conn.Close()
		} else {
			fmt.Printf("Cliente %s no está disponible\n", client)
		}
	}
	return availableClients
}
