from flask import Flask, escape, render_template, request
# from service import QueryService
from models import sqlService, kafkaService
import re
app = Flask(__name__)   

# Root path that returns home.html
# @app.route('/')
# def home():
#     return render_template("home.html")

# About page
@app.route('/about')
def about():
    return render_template("about.html")

# Execute one (or multiple) quries
@app.route('/query')
def query():
    ss = sqlService()
    # url = request.args.get('URL')
    rs = ss.query_url()
    return render_template("query.html", result=rs, content_type='application/json')

# show stream of tweets and get ready for quries, on click go to guery page with url ready
@app.route('/stream')
def stream():
    # num = request.args.get('num_tweets')
    ks = kafkaService()
    ts = ks.fetch_stream(10)

    return render_template("stream.html", tweets=ts, content_type='application/json')

@app.route('/')
def tweets():
    # num = request.args.get('num_tweets')
    ks = kafkaService()
    rs = ks.fetch_results(10)
    return render_template("tweets.html", results = rs, content_type='application/json')


@app.route('/details/<eid>', methods=['GET', 'POST'])
def details(eid):
    # detail= request.form[eid]
    list = eval(request.form.getlist(eid)[0])
    # for item, val in request.form.items():
    # res = detail.strip('][').split(', ')
    # res = re.split(''',(?=(?:[^']*\'[^']*\')*[^']*$)''', row)
    # list = re.split('\t+', row[-1])
    return render_template('detail.html', detail = list) #, address=detail[-1]
# Route that accepts a username(string) and returns it. You can use something similar
#   to fetch details of a username





@app.route('/post/<int:post_id>')
def show_post(post_id):
    # show the post with the given id, the id is an integer
    return 'Post %d' % post_id

@app.route('/path/<path:subpath>')
def show_subpath(subpath):
    # show the subpath after /path/
    return 'Subpath %s' % escape(subpath)

if __name__ == "__main__":
        app.run(port=5000)