cd /home/pi/Documents/warsaw_flats_api/

git pull

git checkout raspberry-updates

source venv/bin/activate

python3 /home/pi/Documents/warsaw_flats_api/api/convert_to_json.py

git add .

git commit -m "updated json"

git push

cd 
