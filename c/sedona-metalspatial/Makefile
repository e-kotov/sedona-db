CXX = clang++
CXXFLAGS = -std=c++20 -O3 -Wall -Wextra
LDFLAGS = -framework Metal -framework Foundation -framework MetalKit

TARGET = poc_metal
SRCS = main.mm

all: $(TARGET)

$(TARGET): $(SRCS) box_intersection.metal
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $(SRCS) -o $(TARGET)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all run clean
