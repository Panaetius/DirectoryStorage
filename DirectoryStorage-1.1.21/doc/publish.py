#
# Originally written by Martijn Faassen for the z3base project
# (http://codespeak.net/z3/)
#

from docutils import core
import os, urllib2

class Error(Exception):
    pass

class Website(object):
    """A website consists of resources and pages.
    """
    def __init__(self, website_path):
        self._website_path = website_path
        self._resource_infos = []
        self._infos = []

    def registerPages(self, pages, layouter, directory, **kw):
        """Register a set of pages.

        The pages all end up in the same directory and share a layouter.

        Optional keyword arguments are also passed through to all of them.
        """
        dir_path = os.path.join(self._website_path, directory)
        for page in pages:
            path = os.path.join(dir_path, page.getName())
            self._infos.append(Info(page, layouter, path, **kw))

    def registerResources(self, resources, directory):
        """Register a set of resources.

        The resources all end up in the same directory.
        """
        self.registerPages(resources, None, directory)

    def registerReleases(self, resources, directory):
        """Register a set of releases.

        Releases are normal resources, but end up in a special release
        directory.
        """
        directory = os.path.join(directory, 'release')
        self.registerResources(resources, directory)
        
    def save(self):
        """Save all pages and resources.
        """    
        for info in self._infos:
            info.save()

class Info(object):
    """Information describing what to do with a web page.

    i.e. how to layout it, where to place it when done.
    """
    def __init__(self, resource, layouter, destination_path, **kw):
        self._resource = resource
        self._destination_path = destination_path
        self._layouter = layouter
        self._kw = kw
         
    def render(self):
        return self._resource.render(self._layouter, **self._kw)

    def save(self):
        """Save this resource.
        """
        try:
            os.makedirs(os.path.dirname(self._destination_path))
        except os.error:
            pass
        data = self.render()
        f = file(self._destination_path, 'w')
        f.write(data)
        f.close()

class BaseResource(object):
    """Base class of all resources.
    """
    def __init__(self, name):
        self._name = name
        
    def getName(self):
        """Name of resource when written.
        """
        return self._name
    
    def render(self, layouter, **kw):
        raise NotImplementedError
    
class BaseDataResource(BaseResource):
    """A resource that gets its main data from a data source.
    """
    def __init__(self, data_source, name=None):
        super(BaseDataResource, self).__init__(name)
        if self._name is None:
            self._name = data_source.getName()
        self._data = data_source.getData()
        
class Resource(BaseDataResource):
    """A resource that is just a file.
    """    
    def render(self, layouter, **kw):
        return self._data

class FileResource(Resource):
    """Convenience way to create a Resource from file.
    """
    def __init__(self, path, name=None):
        super(FileResource, self).__init__(PathSource(path), name)
        
class BasePage(BaseDataResource):
    """Base class of all pages.
    """
    def __init__(self, data_source, name=None):
        super(BasePage, self).__init__(data_source, name)
        self._name = os.path.splitext(self._name)[0] + '.html'
        
    def getData(self):
        """Returns a dictionary with data to use in the page.
        """
        raise NotImplementedError
    
    def render(self, layouter, **kw):        
        kw.update(self.getData())
        return layouter.render(**kw)

class RstPage(BasePage):
    def getData(self):
        return html_parts(self._data, initial_header_level=2)

class FileRstPage(RstPage):
    """Convenience way to create a RstPage from file.
    """
    def __init__(self, path, name=None):
        super(RstPage, self).__init__(PathSource(path), name)
        
class SimplePage(BasePage):
    def __init__(self, name):
        self._name = name + '.html'
        
    def getData(self):
        return {}
    
class SimpleLayouter(object):
    """Simple layouter which replaces {{foo}} in a template with values.
    """
    def __init__(self, template, **kw):
        self._template = template
        self._kw = kw
        
    def render(self, **kw):
        kw.update(self._kw)
        template = self._template
        for key, value in kw.items():
            if type(value) in (str, unicode):
                template = template.replace('{{%s}}' % key, value)
            elif type(value) == type([]):
                l = []
                l.append('<ul>\n')
                for name, url in value:
                    l.append('  <li><a href="%s">%s</a></li>\n' % (url, name))
                l.append('</ul>\n')
                template = template.replace('{{%s}}' % key, ''.join(l))
        return template

class FileSimpleLayouter(SimpleLayouter):
    """A layouter which loads its template from file.
    """
    def __init__(self, path, **kw):
        data = file(path).read()
        super(FileSimpleLayouter, self).__init__(data, **kw)
        
def html_parts(input_string, source_path=None, destination_path=None,
               input_encoding='unicode', doctitle=1, initial_header_level=1):
    """
    Given an input string, returns a dictionary of HTML document parts.

    Dictionary keys are the names of parts, and values are Unicode strings;
    encoding is up to the client.

    Parameters:

    - `input_string`: A multi-line text string; required.
    - `source_path`: Path to the source file or object.  Optional, but useful
      for diagnostic output (system messages).
    - `destination_path`: Path to the file or object which will receive the
      output; optional.  Used for determining relative paths (stylesheets,
      source links, etc.).
    - `input_encoding`: The encoding of `input_string`.  If it is an encoded
      8-bit string, provide the correct encoding.  If it is a Unicode string,
      use "unicode", the default.
    - `doctitle`: Disable the promotion of a lone top-level section title to
      document title (and subsequent section title to document subtitle
      promotion); enabled by default.
    - `initial_header_level`: The initial level for header elements (e.g. 1
      for "<h1>").
    """
    overrides = {'input_encoding': input_encoding,
                 'doctitle_xform': doctitle,
                 'initial_header_level': initial_header_level}
    parts = core.publish_parts(
        source=input_string, source_path=source_path,
        destination_path=destination_path,
        writer_name='html', settings_overrides=overrides)
    return parts


class PathSource(object):
    def __init__(self, path):
        self._path = path
        
    def getName(self):
        return os.path.basename(self._path)
    
    def getData(self):
        f = file(self._path)
        data = f.read()
        f.close()
        return data

class UrlSource(object):
    def __init__(self, url):
        self._url = url
        
    def getName(self):            
        i = self._url.rfind('/')
        return self._url[i+1:]

    def getData(self):
        try:
            f = urllib2.urlopen(self._url)
        except urllib2.URLError:
            raise Error, "Unknown url: %s" % self._url
        data = f.read()
        f.close()
        return data
