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
)

// Definición de estructuras
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

// Mutex global para evitar condiciones de carrera
var mu sync.Mutex

func main() {
	// Inicia servidor TCP en el puerto 9000
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		fmt.Println("Error al iniciar el servidor:", err)
		return
	}
	defer listener.Close()
	fmt.Println("Servidor del Cluster escuchando en el puerto 9000")

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
	fmt.Println("Conexión recibida de la API")

	// Leer configuración de la API
	message, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("Error al leer configuración:", err)
		return
	}

	var config struct {
		Categories []string `json:"categories"`
		MaxResults int      `json:"max_results"`
	}
	err = json.Unmarshal([]byte(strings.TrimSpace(message)), &config)
	if err != nil {
		fmt.Println("Error al decodificar configuración:", err)
		return
	}

	fmt.Printf("Configuración recibida: Categorías=%v, Máximo Resultados=%d\n", config.Categories, config.MaxResults)

	// Procesar datos y generar recomendaciones
	recommendations := processCluster(config.Categories, config.MaxResults)

	// Enviar las recomendaciones de vuelta a la API
	response, _ := json.Marshal(recommendations)
	fmt.Fprintf(conn, "%s\n", response)
	fmt.Println("Resultados enviados a la API")
}

func processCluster(categories []string, maxResults int) []Result {
	// Leer archivo CSV y procesar datos
	products, productCategories, err := readCSV("amazon_reviews_cleaned.csv")
	if err != nil {
		fmt.Println("Error al leer archivo CSV:", err)
		return nil
	}

	// Filtrar productos y calcular resultados
	filteredProducts := filterByCategories(products, categories)
	userItemMatrix := buildUserItemMatrix(filteredProducts)
	finalResults := calculateAverage(buildScores(userItemMatrix), productCategories)

	// Ordenar y limitar resultados
	sort.Slice(finalResults, func(i, j int) bool {
		return finalResults[i].Stars > finalResults[j].Stars
	})

	if maxResults > len(finalResults) {
		maxResults = len(finalResults)
	}
	return finalResults[:maxResults]
}

// Funciones auxiliares para leer CSV, filtrar y calcular datos (idénticas a tu código)

// Leer CSV y parsear a una lista de productos, construyendo el mapa de categorías
func readCSV(filePath string) ([]Product, map[string]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, err
	}

	var products []Product
	productCategories := make(map[string]string)

	for _, record := range records[1:] { // Ignorar la primera fila (cabeceras)
		stars, _ := strconv.ParseFloat(record[2], 64)
		product := Product{
			ReviewerID:      record[0],
			ProductID:       record[1],
			Stars:           stars,
			ProductCategory: record[3],
		}
		products = append(products, product)

		// Rellenar el mapa de categorías
		productCategories[product.ProductID] = product.ProductCategory
	}
	return products, productCategories, nil
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

// Generar puntuaciones simuladas
func buildScores(matrix map[string]map[string]float64) map[string][]float64 {
	scores := make(map[string][]float64)
	for _, items := range matrix {
		for productID, rating := range items {
			scores[productID] = append(scores[productID], rating)
		}
	}
	return scores
}

// Calcular el promedio de las puntuaciones y asignar categorías
func calculateAverage(productScores map[string][]float64, productCategories map[string]string) []Result {
	var finalResults []Result

	for productID, scores := range productScores {
		var sum float64
		for _, score := range scores {
			sum += score
		}
		average := sum / float64(len(scores))

		// Buscar la categoría correspondiente al ProductID
		category, exists := productCategories[productID]
		if !exists {
			category = "unknown" // Asignar categoría desconocida si no se encuentra
		}

		finalResults = append(finalResults, Result{
			ProductID: productID,
			Stars:     average,
			Category:  category,
		})
	}

	return finalResults
}
