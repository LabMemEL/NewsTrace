from flask import Flask, escape, render_template
# from service import QueryService
from models import sqlService, kafkaService
app = Flask(__name__)   

# Root path that returns home.html
@app.route('/')
def home():
    return render_template("home.html")

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

@app.route('/results')
def result():
    # num = request.args.get('num_tweets')
    ks = kafkaService()
    rs = ks.fetch_results(10)
    return render_template("results.html", results = rs, content_type='application/json')

# Route that accepts a username(string) and returns it. You can use something similar
#   to fetch details of a username
@app.route('/user/<username>')
def show_user_profile(username):
    # show the user profile for that user
    return 'User %s' % escape(username)

@app.route('/userwithslash/<username>/')
def show_user_profile_withslash(username):
    # show the user profile for that user
    return 'User %s' % escape(username)

@app.route('/post/<int:post_id>')
def show_post(post_id):
    # show the post with the given id, the id is an integer
    return 'Post %d' % post_id

@app.route('/path/<path:subpath>')
def show_subpath(subpath):
    # show the subpath after /path/
    return 'Subpath %s' % escape(subpath)
