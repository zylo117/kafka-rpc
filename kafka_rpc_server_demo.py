from kafka_rpc import KRPCServer


# Part1: define a class
class Sum:
    def add(self, x, y):
        return x + y


# Part2: instantiate a class to an object
s = Sum()

# assuming you kafka broker is on 0.0.0.0:9092
krs = KRPCServer('0.0.0.0', 9092, s, topic_name='sum', verify=True, encrypt='hanser')
krs.server_forever()
