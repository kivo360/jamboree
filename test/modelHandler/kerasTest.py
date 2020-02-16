from numpy import loadtxt
from keras.models import Sequential
from keras.layers import Dense
from keras import optimizers
from redis import Redis
from jamboree.utils.helper import Helpers
import pickle
import Model_Handler

redis = Redis()


model = Sequential()
model.add(Dense(12, input_dim=8, activation='relu'))
model.add(Dense(8, activation='relu'))
model.add(Dense(1, activation='sigmoid'))
optimizer = optimizers.SGD(lr=0.01, decay=1e-6, momentum=0.9, nesterov=True)

def main():
	handler = Model_Handler.ModelHandler(redis)
	handler.save_keras("example_key2", model, optimizer, 20)
	loadedModel, loadedOptimizer = handler.load_latest_model("example_key2")

main()