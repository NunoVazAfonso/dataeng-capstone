cd {{ var.value.input_folder }}/data/

# Demo purposes select only a few lines
head -2 flights_list > flights
head -10 twitter_list > tweets

echo "Starting file downloads"

echo 'Starting flight data download'
wget -i flights

echo 'Starting tweet data download'
wget -i tweets

echo 'Finished file downloads' 