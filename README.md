**Note**: Very rudimentary and slightly mobile specific. Eventually stuff will
be refactored out of here to be a generic thing that everyone doing EL -> Limn
can use


## Installing dependencies

You will need [Limn](https://github.com/wikimedia/limn) itself.

To run the `generate.py` script you should create a Python virtualenv, activate
it and install all the required dependencies:

    $ virtualenv env
    $ source env/bin/activate
    $ pip install -r requirements.txt

After that, every time when you want to run `generate.py` you only need to
activate the virtualenv with all the dependencies already installed by running
`source env/bin/activate`.

Later you need to link the data directory to the Limn instance. Assuming that
you have Limn cloned to `~/limn` and this project to `~/limn-mobile-data`,
you will need to run the following command (only once):

    $ cd ~/limn
    $ coke --vardir ./var --data ../limn-mobile-data --to mobile link_data

Then you should be able to start Limn by running `npm start` and see it in
action at `http://localhost:8081`.

### Mac OS X

You will need to install some header files for some of the Python dependencies
to compile. To do that it's best if you install MySQL and libyaml using
Homebrew:

    $ brew install mysql
    $ brew install libyaml

### Ubuntu

On Ubuntu the following does the trick:

    $ sudo apt-get install libmysqlclient-dev libyaml-dev


## Adding your own Graphs

(Specific to Mobile right now, limitation will be removed *soon*)

- Write an SQL Query that returns data in the appropriate format, and place it
  in `mobile/<name>.sql`
- Add `<name>` to appropriate position in `dashboards/reportcard.json`
- Run `generate.py mobile` to generate required metadata *and* data (run
  `generate.py -h` for details)
- Deploy to limn! (Ask analytics to get you access)


## Testing using local data

By default the instance you run will show graphs using production data.
To generate the data for your local instance you need to make the analytics
databases available to your local machine. If you have access to stat1, you
can do that by running:

    $ scripts/ssh

in a separate terminal window and leaving it open.

Then, you should create a file called `scripts/my.cnf.research` with the
following content:

    [client]
    user=[analytics DB user]
    password=[analytics DB password]

Now, you should be able to run `generate.py` with config overrides:

    $ python generate.py -c scripts/config.yaml mobile

When all the data is generated you still need to do one more thing to let
Limn know that it should use the local data. The hacky solution is to replace
all the data URLs temporarily. You can do it by running:

    $ scripts/localurl

Now, you should be able to do:

    $ cd ~/limn
    $ npm start

and see your local instance data at `http://localhost:8081`. You have to
remember to replace all the local URLs to remote URLs before pushing your
changes though by running:

    $ scripts/remoteurl

