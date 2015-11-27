
How to do a new release
===

```bash
cd ..
rm -rf ppa
bzr branch lp:~discnt/+junk/discnt ppa
cd ppa
find . -not -wholename '*.bzr*' -delete
rsync -r ../discnt/ .
rm -rf .git
make distclean
```

Now you should have all the changes, check, commit and push them:
```bash
bzr status
bzr add *
bzr commit
bzr push lp:~discnt/+junk/discnt
```

After this go to the [recipe page](https://code.launchpad.net/~discnt/+recipe/discnt-stable) and request a build.

