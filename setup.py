from distutils.core import setup
setup(
  name = 'tidysqlite',
  packages = ['tidysqlite'],
  version = '0.0.1',
  license='MIT',
  description = 'A tidy data method for manipulating a sqlite repository in python',
  author = 'Eric Dunford',
  author_email = 'ethomasdunford@gmail.com',
  url = 'https://github.com/edunford/tidysqlite',
  download_url = 'https://github.com/joelbarmettlerUZH/Scrapeasy/archive/pypi-0_1_3.tar.gz',
  keywords = ['sql', 'tidy', 'data','wrangling'],
  install_requires=[
          'pandas',
          'sqlite3',
      ],
  classifiers=[  # Optional
    # How mature is this project? Common values are
    #   3 - Alpha
    #   4 - Beta
    #   5 - Production/Stable
    'Development Status :: 3 - Alpha',

    # Indicate who your project is intended for
    'Intended Audience :: Data Scienct :: Social Science',

    # Pick your license as you wish
    'License :: OSI Approved :: MIT License',

    # Specify the Python versions you support here. In particular, ensure
    # that you indicate whether you support Python 2, Python 3 or both.
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
  ],
)