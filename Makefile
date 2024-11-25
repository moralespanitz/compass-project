main:
	g++ -o main  main.cpp -I/opt/homebrew/opt/libpq/include -L/opt/homebrew/opt/libpq/lib -lpq -std=c++17 && ./main

postgres:
	g++ -o postgres  postgres.cpp -I/opt/homebrew/opt/libpq/include -L/opt/homebrew/opt/libpq/lib -lpq -std=c++17 && ./postgres

clean:
	rm -rf postgres main
