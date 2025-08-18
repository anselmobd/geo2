class PrinterTask:
    def __init__(self, text):
        self.text = text

    def run(self, inputs=None, outputs=None):
        print(self.text)
