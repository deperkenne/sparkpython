
class DropColumnNotExistException(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return "this dropColumn:" + self.message + " " + "not exist"


class ColumnNotExistException(Exception):
    def __init__(self,message):
        self.message = message
    def __str__(self):
        return self.message


class DataFrameNotFoundException(Exception):

    def __init__(self,name="",message=""):
        self.name = name
        self.message = message
    def __str__(self):
        return str(self.name) + " "+ str(self.message)

class NotAStringException(Exception):
        def __init__(self,  name="",message=""):
            self.name = name
            self.message = message

        def __str__(self):
            return str(self.name) + " " + str(self.message)