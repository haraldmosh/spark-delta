import string
import tempfile
from random import randint, choice, choices
import csv
from datetime import datetime
import calendar
import time
import os


def generate_securities(generator_conf):
    alphanumeric_upper_chars = string.digits + string.ascii_uppercase

    def _new_name():
        return ''.join(choice(string.ascii_letters) for x in range(randint(3, 20)))

    def _new_cusip():
        return ''.join(choices(alphanumeric_upper_chars, k=9))

    def _new_description():
        num_words = randint(3, 6)
        words = []
        for word_pos in range(num_words):
            num_letters_in_word = randint(3, 8)
            word = ''.join(choices(string.ascii_letters, k=num_letters_in_word))
            words.append(word)
        return ' '.join(words)

    header = ["id", "name", "description", "cusip"]
    output_root = generator_conf['output_root']
    num_files = generator_conf['num_files']

    def _write_to_filesystem():
        last_id = generator_conf['last_id']

        for i in range(num_files):
            current_timestamp = calendar.timegm(datetime.utcnow().utctimetuple())

            with open(f"{output_root}/security_{current_timestamp}.csv", 'a+') as f:
                writer = csv.writer(f)
                writer.writerow(header)

                for x in range(1, randint(2, 6)):
                    last_id += 1
                    writer.writerow([last_id, _new_name(), _new_description(), _new_cusip()])

            time.sleep(0.2)
        return last_id

    if generator_conf["output_type"] == "filesystem":
        return _write_to_filesystem()


def get_last_written_security_id(last_written_id_file):
    if os.path.exists(last_written_id_file):
        with open(last_written_id_file, "r") as last_id_file:
            last_id = int(last_id_file.readline())
    else:
        last_id = 0
    return last_id


def update_last_written_security_id(last_written_id_file, latest_id):
    with open(last_written_id_file, "w+") as last_id_file:
        last_id_file.write(str(last_written_id))


def generate_positions(generator_conf):
    pass


if __name__ == '__main__':
    with tempfile.TemporaryDirectory() as tmpdirname:
        securities_root = '/tmp/delta_output/securities'
        last_written_id_file = f"{securities_root}/last_written_id.txt"

        security_conf = {
            "output_root": securities_root,
            "output_type": "filesystem",
            "last_id": get_last_written_security_id(last_written_id_file),
            "num_files": 2,
        }
        last_written_id = generate_securities(security_conf)
        update_last_written_security_id(last_written_id_file, last_written_id)

        position_conf = {

        }
        generate_positions(position_conf)
