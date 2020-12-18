from flask import Flask
app = Flask(__name__)
@app.route("/",methods=['POST'])
def home():
    print('Hello World')
app.run()

