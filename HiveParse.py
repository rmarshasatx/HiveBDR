

import argparse



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Perform BDR jobs while getting around BDR limitations.')
    parser.add_argument("username", help="user to run the BDR jobs")
    parser.add_argument("password", help="password of the user")
    parser.add_argument("database", help="database to replicate from source to target cluster using BDR")
    parser.add_argument("tables", nargs='*',help="optional: specific subset of tables to replicate")
    args = parser.parse_args()

    print args.database, args.tables
    exit(0)

