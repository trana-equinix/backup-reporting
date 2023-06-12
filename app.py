from flask import Flask, render_template
from testing import get_data
app = Flask(__name__)

@app.route('/')
def index():
    # Assuming your parsed_job_data is a list of dictionaries
    parsed_job_data = get_data()

    return render_template('index.html', data=parsed_job_data)

if __name__ == '__main__':
    app.run(debug=True)