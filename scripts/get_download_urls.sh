echo 'Starting URLs download'

zenodo_get -o {{ var.value.input_folder }}/data/ -w flights_list 4485741

zenodo_get -o {{ var.value.input_folder }}/data/ -w twitter_list 4568860

echo 'Finished URLs download' 