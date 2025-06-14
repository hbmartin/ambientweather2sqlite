class UnexpectedEmptyDictionaryError(Exception):
    def __init__(self):
        super().__init__("Dictionary is unexpectedly empty")
