import flask
import yaml

from convert_to_json import get_flats_stats

app = flask.Flask(__name__)
app.config["DEBUG"] = True

with open(r'../config.yaml') as f:
    paths = yaml.safe_load(f)


def main():
    @app.route('/', methods=['GET'])
    def home():
        json_data = get_flats_stats(paths['data_path'])
        return json_data

    app.run()


if __name__ == '__main__':
    main()
