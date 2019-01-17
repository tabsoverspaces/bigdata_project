# import re

def filter_body_noise(input):

    # count blocks of code
    nr_code_blocks = len(re.findall(r"</code>", input))
    # count link in body
    nr_links = len(re.findall(r"</a>", input))

    # couldnt make the regex work with multi-line files, so I remove them :))))
    input = input.replace('\n', ' ')
    input = input.replace("\'", '')

    # remove code blocks from body
    input = re.sub(r"<code>(.*?)</code>", "", input,flags=re.MULTILINE)

    # remove all other tags from body
    input = re.sub(r"<[^>]*>", "", input)

    # remove hardcoded links
    input = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', input, flags=re.MULTILINE)

    # remove multiple whitespace characters to clean up the previous
    # string substitutions(because when removing for example a code block, there will be two spaces
    # remaining on each side of the code block, thus we get two whitespace characters
    input = re.sub("\s\s+", ' ', input)

    # count remaining words
    nr_words = len(input.split(' '))

    # returns tuple in format (number_of_words, number_of_links, number_of_code_blocks)
    return (nr_words, nr_links, nr_code_blocks)
