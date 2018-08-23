


PACKAGE="plumpy"
VERSION_FILE=${PACKAGE}/version.py

version=$1
while true; do
    read -p "Release version ${version}? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done


sed -i "1 s/__version__* =.*/__version__ = \"${version}\"/" $VERSION_FILE

current_branch=`git rev-parse --abbrev-ref HEAD`

tag="v${version}"
relbranch="release-${version}"

echo Releasing version $version

git checkout -b $relbranch 
git add ${VERSION_FILE}
git commit -m "Release ${version}"


# Merge into master

git checkout master
git merge --no-ff $relbranch
# Tag the thing
git tag -a $tag -m "Version $version"

git checkout $current_branch
git merge master

git branch -d $relbranch

# Push everything
git push --tags origin master
git push origin $current_branch

# Release on pypi
rm -r dist
rm -r build
rm -r *.egg-info
python setup.py sdist
python setup.py bdist_wheel --universal

twine upload dist/*




