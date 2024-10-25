import csv


class CSVWriter:

    def write_to_csv(self, products, filename=None):
        fieldnames = products[0].keys()
        print(f"===> Writing to {filename}")
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(
                csvfile, delimiter=",", fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(products)

        print(f"Finish writing!")
