
class Printer:

    def __init__(self, file):
        self.cache = []
        self.file = file

    def log(self, line):
        print(line)
        self.cache.append(line)

    def finalWrite(self, line):
        with open(self.file, 'w+') as f:
            for line in self.cache:
                f.write(line)
