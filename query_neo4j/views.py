from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from query_neo4j import haokeywords
from query_neo4j import searchnode
from query_neo4j import searchgraph
from query_neo4j import searchindex
from query_neo4j import clicknode
from query_neo4j import countnl
from query_neo4j import nlcount
from query_neo4j import querynode
from query_neo4j import searchname
from query_neo4j import searchpath1
from query_neo4j import searchpath1EN
from query_neo4j import searchpathSimple
from query_neo4j import searchpath2
from query_neo4j import clickfuse1temp
from query_neo4j import searchfuse3
from query_neo4j import gptqa
from query_neo4j import deletenode
from query_neo4j import addnode
# from query_neo4j import updatenode
from query_neo4j import searchexamplepath
from query_neo4j import setpath
from query_neo4j import searchmultipath
from query_neo4j import searchhypernode
import json
from query_neo4j import entityLink
from query_neo4j import download 
from query_neo4j import downindex
from query_neo4j import wikiSearchUrls
from query_neo4j import pageImport
from query_neo4j import deleteprivate
from query_neo4j import loaduserfile
from query_neo4j import downloaduserfile
from query_neo4j import deleteuserfile
from query_neo4j import multidelete
from query_neo4j import changestatus
from query_neo4j import getinfofilelist
from query_neo4j import changepicture
from query_neo4j import updatenode
from query_neo4j import saveEnhancedNode
from query_neo4j import saveEnhancedNodeAdmin
from query_neo4j import saveApproved
from query_neo4j import saveEnhancedNodeAdmin1
from query_neo4j import saveEnhancedNode1
from query_neo4j import saveEnhancedNodeAdmin2
from query_neo4j import saveEnhancedNode2
from query_neo4j import upload
from query_neo4j import uploadneo4j
from query_neo4j import spring_jiekou
from query_neo4j import spring_download
from query_neo4j import checkxiaoqi
from query_neo4j import check_user_identity
from query_neo4j import update_entity
from query_neo4j import up_alter_data
from query_neo4j import get_alter_data
from query_neo4j import change_alter_data
from query_neo4j import get_newest_xiaoqi_id
from query_neo4j import file_search_folder
from query_neo4j import disambiguation
from query_neo4j import changefiledata
from query_neo4j import changenodedata
from query_neo4j import search_urls
from query_neo4j import crawl_pages
from query_neo4j import create_entity
from query_neo4j import auto_recommendtion
from query_neo4j import knowledge_subscription
from query_neo4j import change_directory
from query_neo4j import contents_gen_manage
from query_neo4j import File_Management
from query_neo4j import upload_temp
def index(request):
    # return HttpResponse("neo4j results")
    return HttpResponse(haokeywords.main(request))


def searchNode(request):
    return HttpResponse(searchnode.main(request))

def searchGraph(request):
    return HttpResponse(searchgraph.main(request))

def searchIndex(request):
    return HttpResponse(searchindex.main(request))

def downLoad(request):
    return HttpResponse(download.main(request))


def downIndex(request):
    return HttpResponse(downindex.main(request))
def WikiSearchUrls(request):
    return HttpResponse(wikiSearchUrls.main(request))

def PageImport(request):
    return HttpResponse(pageImport.main(request))

def DeletePrivate(request):
    return HttpResponse(deleteprivate.main(request))

def LoadUserFile(request):
    return HttpResponse(loaduserfile.main(request))

def DownloadUserFile(request):
    return HttpResponse(downloaduserfile.main(request))

def DeleteUserFile(request):
    return HttpResponse(deleteuserfile.main(request))

def MultiDelete(request):
    return HttpResponse(multidelete.main(request))

def ChangeStatus(request):
    return HttpResponse(changestatus.main(request))

def getInfoFileList(request):
    return HttpResponse(getinfofilelist.main(request))

def changePicture(request):
    return HttpResponse(changepicture.main(request))

def Spring_Jiekou(request):
    return HttpResponse(spring_jiekou.main(request))

def Spring_Download(request):
    return HttpResponse(spring_download.main(request))

def checkXiaoqi(request):
    return HttpResponse(checkxiaoqi.main(request))

def Check_user_identity(request):
    return HttpResponse(check_user_identity.main(request))

def Up_alter_data(request):
    return HttpResponse(up_alter_data.main(request))

def Get_alter_data(request):
    return HttpResponse(get_alter_data.main(request))

def Change_alter_data(request):
    return HttpResponse(change_alter_data.main(request))

def Get_newest_xiaoqi_id(request):
    return HttpResponse(get_newest_xiaoqi_id.main(request))

def Update_entity(request):
    return HttpResponse(update_entity.main(request))

def File_search_folder(request):
    return HttpResponse(file_search_folder.main(request))

def clickNode(request):
    return HttpResponse(clicknode.main(request))


def countNL(request):
    return HttpResponse(countnl.main(request))


def NLcount(request):
    return HttpResponse(nlcount.main(request))


def queryNode(request):
    return HttpResponse(querynode.main(request))


def searchName(request):
    return HttpResponse(searchname.main(request))


def searchPath1(request):
    return HttpResponse(searchpath1.main(request))


def searchPath2(request):
    return HttpResponse(searchpath2.main(request))

def downLoadIndex(request):
    return HttpResponse(downLoadIndex.main(request))

def searchPath1EN(request):
    return HttpResponse(searchpath1EN.main(request))


def searchPathSimple(request):
    return HttpResponse(searchpathSimple.main(request))


def clickFuse1Temp(request):
    return HttpResponse(clickfuse1temp.main(request))


# 知识搜索
def searchFuse3(request):
    return HttpResponse(searchfuse3.main(request))


def gptQa(request):
    return HttpResponse(gptqa.main(request))


def deleteNode(request):
    return HttpResponse(deletenode.main(request))


def addNode(request):
    return HttpResponse(addnode.main(request))


def Updatenode(request):
    return HttpResponse(updatenode.main(request))


def searchExamplePath(request):
    return HttpResponse(searchexamplepath.main(request))


def setPath(request):
    return HttpResponse(setpath.main(request))


def searchMultiPath(request):
    return HttpResponse(searchmultipath.main(request))


def searchHyperNode(request):
    return HttpResponse(searchhypernode.main(request))


def EntityLink(request):
    return HttpResponse(entityLink.main(request))
def SaveEnhancedNode(request):
    return HttpResponse(saveEnhancedNode.main(request))
def SaveEnhancedNodeAdmin(request):
    return HttpResponse(saveEnhancedNodeAdmin.main(request))

def SaveApproved(request):
    return HttpResponse(saveApproved.main(request))
def SaveEnhancedNodeAdmin1(request):
    return HttpResponse(saveEnhancedNodeAdmin1.main(request))
def SaveEnhancedNode1(request):
    return HttpResponse(saveEnhancedNode1.main(request))
def SaveEnhancedNode2(request):
    return HttpResponse(saveEnhancedNode2.main(request))
def SaveEnhancedNodeAdmin2(request):
    return HttpResponse(saveEnhancedNodeAdmin2.main(request))

def upLoad(request):
    return HttpResponse(upload.main(request))

def uploadNeo4j(request):
    return HttpResponse(uploadneo4j.main(request))

def disamBiguation(request):
    return HttpResponse(disambiguation.main(request))

def changeFileData(request):
    return HttpResponse(changefiledata.main(request))

def changeNodedata(request):
    return HttpResponse(changenodedata.main(request))

def search_urls_view(request):
    return HttpResponse(json.dumps(search_urls.search_urls(request)), content_type='application/json')

def crawl_pages_view(request):
    return HttpResponse(json.dumps(crawl_pages.crawl_pages(request)), content_type='application/json')

def createEntity(request):
    return HttpResponse(create_entity.main(request), content_type='application/json')

def auto_recommend(request):
    result = auto_recommendtion.auto_recommendtion(request)
    return HttpResponse(json.dumps(result, ensure_ascii=False),
                       content_type='application/json')
def knowledge_subscription_view(request):
        return HttpResponse(knowledge_subscription.knowledge_subscription(request), content_type='application/json')
def get_subscriptionInfo_view(request):
    return HttpResponse(knowledge_subscription.get_subscriptionInfo(request), content_type='application/json')
def change_directory_view(request):
    return HttpResponse(change_directory.change_directory(request), content_type='application/json')

def directory_management(request):
    return HttpResponse(contents_gen_manage.directory_management(request), content_type='application/json')

def file_management(request):
    return HttpResponse(File_Management.main(request), content_type='application/json')
def upload_new(request):
    return HttpResponse(upload_temp.upload_and_process(request), content_type='application/json')