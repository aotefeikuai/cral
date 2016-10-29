import urllib
import re
global x
x=0
def getHtml(url):
    page=urllib.urlopen(url)
    html=page.read()
    return html

def getIma(html):
    global x
    reg=r'href="(.+?\.html)" class='
    urlre=re.compile(reg)
    urllist=re.findall(urlre,html)
    for imgurl in urllist:
        htm = getHtml('http://rosioo.com/'+imgurl)
        reg = r'src=" (.+?\.jpg)" title='
        imgre = re.compile(reg)
        imglist = re.findall(imgre, htm)

        for imga in imglist:
            urllib.urlretrieve(imga, '%s.jpg' % x)
            x += 1
for num in range(1,20):
    html=getHtml('http://rosioo.com/shipin/list_10_'+str(num)+'.html')
    print getIma(html)
