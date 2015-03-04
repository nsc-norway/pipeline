import argparse
import subprocess


from genologics.lims import *
import nsc


'''
Cleanup script:


directory=$1
echo "......"
read -r -p "Do you want to clean $directory? [Y/n]" response
response=${response,,}
if [[ $response =~ ^(no|n) ]]; then
	echo "You aborted this delete"
fi
echo "Cleaning started for $directory ....."
echo "......"
cd $directory 

echo "Delecting Thumbnail images"
rm -r Thumbnail_Images
echo "Done"
echo

echo "Delecting Data/Intensities/L00* (*.clocs)"
cd Data/Intensities
ls | grep "L00" |xargs rm -r
echo "Done"
echo

echo "Delecting Data/Intensities/BaseCalls/L00* (bcl and stats)"
cd BaseCalls
ls | grep "L00" |xargs rm -r
echo "Done"
echo

echo "Done cleaning $directory"

'''



def main():
    print 'Here we are, in move-files'

    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('nsc')

    main()

