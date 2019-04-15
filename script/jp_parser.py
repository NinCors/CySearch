jp_dict_file = "../src/main/resources/freq_dict_jp_original.txt"
jp_dict_file_output = "../src/main/resources/freq_dict_jp.txt"

def jp_dict_parse():
    output = open(jp_dict_file_output, "a")

    with open(jp_dict_file) as file:
        lines = file.readlines()

    for line in lines:
        words = line.split()
        newline = words[2] + " " + words[1] + "\n"
        print(newline)
        output.write(newline)

    file.close()
    output.close()

jp_dict_parse()
