from flask import Flask,render_template
app = Flask(__name__)
@app.route("/")
def home():
    print('Hello World')
    return render_template("Index.html")
app.run()

