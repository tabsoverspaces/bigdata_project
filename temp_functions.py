# use this function to filter noise from each post's body
def filter_body_noise(input):

    # count blocks of code
    code_blocks = len(re.findall(r"</code>", input))

    # couldnt make the regex work with multi-line files, so I remove them :))))
    input = input.replace('\n', ' ')
    input = input.replace("\'", '')

    # remove code blocks from body
    input = re.sub(r"<code>(.*?)</code>", "", input,flags=re.MULTILINE)

    # remove all other tags from body
    input = re.sub(r"<[^>]*>", "", input)

    # remove hardcoded links
    input = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', input, flags=re.MULTILINE)

    # returns tuple in format (filtered_body, number_of_code_blocks)
    return (input,code_blocks)
