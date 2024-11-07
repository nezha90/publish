package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// 定义数据结构
type Record struct {
	PieceCID   string
	PayloadCID string
	Size       string
	CarSize    string
}

// 数据库中的记录结构
type PublishRecord struct {
	PieceCID string
	LDN      string
	MinerID  string
}

// 连接数据库
func connectDB() (*sql.DB, error) {
	dsn := "root:ccyy123456@tcp(localhost:3306)/filecoin"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// 从数据库获取符合 piece_cid 的记录
func fetchPublishRecords(db *sql.DB, pieceCID string) ([]PublishRecord, error) {
	query := "SELECT piece_cid, ldn, miner_id FROM publish WHERE piece_cid = ?"
	rows, err := db.Query(query, pieceCID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []PublishRecord
	for rows.Next() {
		var record PublishRecord
		if err := rows.Scan(&record.PieceCID, &record.LDN, &record.MinerID); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// 解析文件
func parseFile(filename string) ([]Record, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []Record
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		record := Record{
			PieceCID:   fields[0],
			PayloadCID: fields[1],
			Size:       fields[2],
			CarSize:    fields[3],
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

// 检查重复项
func checkDuplicates(records []Record, db *sql.DB, minerID, ldn string) (map[string][]string, error) {
	duplicateResults := make(map[string][]string)
	for _, record := range records {
		publishRecords, err := fetchPublishRecords(db, record.PieceCID)
		if err != nil {
			return nil, err
		}

		for _, pubRecord := range publishRecords {
			key := "正常状态"
			if pubRecord.LDN == ldn && pubRecord.MinerID == minerID {
				key = "这个piece cid 已经发过"
			} else if pubRecord.LDN != ldn {
				key = "这个piece cid 被别的 ldn 发布"
			}

			duplicateResults[key] = append(duplicateResults[key], record.PieceCID)
		}
	}
	return duplicateResults, nil
}

// 将数据写入数据库
func writeToDB(db *sql.DB, records []Record, ldn, minerID string) error {
	for _, record := range records {
		_, err := db.Exec("INSERT INTO publish (piece_cid, miner_id, ldn) VALUES (?, ?, ?)", record.PieceCID, minerID, ldn)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: program <filename> <ldn> <miner_id>")
		return
	}

	filename := os.Args[1]
	ldn := os.Args[2]
	minerID := os.Args[3]

	// 连接数据库
	db, err := connectDB()
	if err != nil {
		log.Fatalf("Database connection error: %v", err)
	}
	defer db.Close()

	// 解析文件
	records, err := parseFile(filename)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	// 检查重复项
	duplicateResults, err := checkDuplicates(records, db, minerID, ldn)
	if err != nil {
		log.Fatalf("Error checking duplicates: %v", err)
	}

	// 输出结果
	for category, pubRecords := range duplicateResults {
		fmt.Printf("\n%s:\n", category)
		if category == "正常状态" {
			fmt.Printf("num: %d\n", len(pubRecords))
		}
		fmt.Printf("PieceCID:\n %s", pubRecords)
	}
	fmt.Println()

	// 等待用户输入
	fmt.Println("Do you want to save these records to the database? (yes/no)")
	var input string
	fmt.Scanln(&input)
	if strings.ToLower(input) == "yes" {
		// 写入数据库
		err = writeToDB(db, records, ldn, minerID)
		if err != nil {
			log.Fatalf("Error writing to database: %v", err)
		}
		fmt.Println("Records saved to database.")
	} else {
		fmt.Println("Operation aborted.")
	}
}
