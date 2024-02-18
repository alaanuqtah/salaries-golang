package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gofiber/fiber/v2"
	_ "github.com/lib/pq"
)

type Emp struct {
	ID     int    `json:id`
	Name   string `json:name`
	Age    int    `json:age`
	Salary int    `json:salary`
}

func main() {
	//connect to postgress db
	db, err := sql.Open("postgres", "postgres://jxznhuyj:7lukcbyIsYxYrK2PlGxG25_pP8-o6Fw5@trumpet.db.elephantsql.com/jxznhuyj")
	if err != nil {
		log.Fatal("Error connecting to database: %v", err)
	}
	defer db.Close()

	//populate db
	//     1. create the table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS employees( 
		id SERIAL PRIMARY KEY,
		name VARCHAR(10),
		age INTEGER,
		salary INTEGER
	)`)
	if err != nil {
		log.Fatal("Error creating table:%v", err)
	}
	//    2. put values in the table
	employees := []Emp{
		{Name: "Alice", Age: 25, Salary: 50000},
		{Name: "Bob", Age: 30, Salary: 60000},
		{Name: "Charlie", Age: 25, Salary: 55000},
		{Name: "Diana", Age: 30, Salary: 65000},
		{Name: "Eva", Age: 35, Salary: 70000},
	}

	for _, emp := range employees {
		_, err := db.Exec(`INSERT INTO employees (name,age,salary) VALUES ($1,$2,$3)`, emp.Name, emp.Age, emp.Salary)
		if err != nil {
			log.Fatal("error inserting data: %v", err)
		}
	}

	app := fiber.New()
	//routes
	//   1.get all emps
	app.Get("/employees", func(c *fiber.Ctx) error {
		employees, err := getEmp(db)
		if err != nil {
			return c.Status(http.StatusInternalServerError).SendString(err.Error())
		}
		return c.JSON(employees)
	})
	//   2.get avg salaries per age group
	app.Get("/employees/avg-salaries", func(c *fiber.Ctx) error {
		avgSalaries, err := getAverageSalaries(db)
		if err != nil {
			return c.Status(http.StatusInternalServerError).SendString(err.Error())
		}
		return c.JSON(avgSalaries)
	})
	//   3.post an emp
	app.Post("employees", func(c *fiber.Ctx) error {
		employee := new(Emp)
		err := c.BodyParser(employee)
		if err != nil {
			return c.Status(http.StatusBadRequest).SendString(err.Error())
		}
		err = createEmp(db, employee)
		if err != nil {
			return c.Status(http.StatusInternalServerError).SendString(err.Error())
		}
		return c.JSON(employee)

	})
	app.Get("employees/:id", func(c *fiber.Ctx) error {
		idstr := c.Params("id")
		id, err := strconv.Atoi(idstr)
		if err != nil {
			panic(err)
		}

		employee, err := getEmpByID(db, id)
		if err != nil {
			return c.Status(http.StatusBadRequest).SendString(err.Error())
		}

		return c.JSON(employee)

	})

	app.Delete("employees/:id", func(c *fiber.Ctx) error {
		idstr := c.Params("id")
		id, err := strconv.Atoi(idstr)
		if err != nil {
			panic(err)
		}

		err = deleteEmployee(db, id)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		return c.SendStatus(fiber.StatusOK)

	})

	app.Listen(":3000")

}

func deleteEmployee(db *sql.DB, id int) error {
	// Prepare the SQL statement for deleting an employee by ID
	stmt, err := db.Prepare("DELETE FROM employees WHERE id = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Execute the SQL statement to delete the employee
	_, err = stmt.Exec(id)
	if err != nil {
		return err
	}

	return nil
}

func getEmpByID(db *sql.DB, id int) (*Emp, error) {
	// Prepare the SQL statement for finding an employee by ID
	stmt, err := db.Prepare("SELECT id, name, age, salary FROM employees WHERE id = $1")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Execute the SQL statement to retrieve the employee by ID
	row := stmt.QueryRow(id)

	// Create a new Employee object to store the retrieved employee data
	var employee Emp
	err = row.Scan(&employee.ID, &employee.Name, &employee.Age, &employee.Salary)
	if err != nil {
		// If the employee with the given ID is not found, return an error
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("Employee with ID %d not found", id)
		}
		return nil, err
	}

	return &employee, nil
}

func createEmp(db *sql.DB, emp *Emp) error {
	_, err := db.Exec("INSERT INTO employees (name,age,salary) VALUES ($1,$2,$3)", emp.Name, emp.Age, emp.Salary)
	return err
}

func getEmp(db *sql.DB) ([]Emp, error) {
	rows, err := db.Query("SELECT id,name,age,salary FROM employees")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var employees []Emp
	for rows.Next() {
		var emp Emp
		err := rows.Scan(&emp.ID, &emp.Name, &emp.Age, &emp.Salary)
		if err != nil {
			return nil, err
		}
		employees = append(employees, emp)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return employees, nil
}

func getAverageSalaries(db *sql.DB) (map[int]int, error) {
	rows, err := db.Query("SELECT age, salary FROM employees")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	salaryCounts := make(map[int][]int)

	for rows.Next() {
		var age, salary int
		err := rows.Scan(&age, &salary)
		if err != nil {
			return nil, err
		}
		salaryCounts[age] = append(salaryCounts[age], salary)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	avgSalaries := make(map[int]int)
	for age, salaries := range salaryCounts {
		avgSalaries[age] = calculateAverage(salaries)
	}

	return avgSalaries, nil
}

func calculateAverage(salaries []int) int {
	sum := 0
	for _, num := range salaries {
		sum += num
	}

	if len(salaries) > 0 {
		return sum / len(salaries)
	}
	return 0

}
