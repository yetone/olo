from setuptools import setup, find_packages, Extension

setup_kwargs = {}

try:
    from Cython.Distutils import build_ext
    setup_kwargs.update(dict(
        ext_modules=[
            Extension(
                'olo.utils',
                sources=['olo/utils.py'],
                extra_compile_args=['-O3'],
                language='c++'
            ),
            Extension(
                'olo._speedups',
                sources=['olo/_speedups.py'],
                extra_compile_args=['-O3'],
                language='c++'
            ),
        ],
        cmdclass={'build_ext': build_ext},
    ))
except ImportError:
    pass

install_requires = []
for line in open('requirements.txt', 'r'):
    install_requires.append(line.strip())

setup(
    name='olo',
    version='0.1.4',
    keywords=('ORM', 'olo', 'cache', 'sqlstore'),
    description='ORM with intelligent and elegant cache manager',
    url='https://github.com/yetone/olo',
    license='MIT License',
    author='yetone',
    author_email='guanxipeng@douban.com',
    packages=find_packages(exclude=['tests.*', 'tests', 'benchmarks']),
    setup_requires=['Cython >= 0.20'],
    install_requires=install_requires,
    platforms='any',
    tests_require=(
        'pytest',
    ),
    **setup_kwargs
)
