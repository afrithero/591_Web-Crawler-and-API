from flask import Flask
from flask_restful import Api, Resource
from resource.query import *

app = Flask(__name__)
app.config["DEBUG"] = True
app.config['JSON_AS_ASCII'] = False

api = Api(app)
api.add_resource(RentAll,"/rent")
api.add_resource(RentTarget,"/rent/<option>")

@app.errorhandler(Exception)
def handle_error(error):
	status_code = 500
	if type(error).__name__ == "NotFound":
		status_code = 404

	return {"code":status_code,"msg":type(error).__name__}

if __name__ == '__main__':
	app.run(host='0.0.0.0',port=5000)