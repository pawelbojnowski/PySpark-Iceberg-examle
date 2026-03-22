def print_header(message: str) -> None:
    width = max(120, len(message) + 2)
    line = "#" * width
    middle = message.center(width)

    print("\n")
    print(line)
    print(middle)
    print(line)
    print("\n")
