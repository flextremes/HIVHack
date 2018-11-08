from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class NLPArticle(Base):
    __tablename__            = 'Articles'
    __tabPK__                = Column('tabPK',          Integer, primary_key = True)
    __lang__                 = Column('lang',           String(10))
    __title__                = Column('title',          String(500))
    __topic_keywords__       = Column('topic_keywords', String(1000))
    __authors__              = Column('authors',        String(1000))
    __authorsDet__           = Column('authorsDet',     String(1000))
    __article_date__         = Column('article_date',   String(30))
    __content__              = Column('content',        String(100000))
    __tokens__               = Column('tokens',         String(100000))
    __topicKeywordsList__    = []
    __tokensList__           = []
    
    def __init__( self, p_tabPK, 
                        p_lang            = None, 
                        p_title           = None,
                        p_topic_keywords  = None,
                        p_authors         = None, 
                        p_authorsDet      = None, 
                        p_article_date    = None, 
                        p_content         = None,
                        p_tokens          = None ):
        self.__tabPK__           = p_tabPK        
        self.__title__           = p_title
        self.__lang__            = p_lang
        self.__topic_keywords__  = p_topic_keywords
        self.__authors__         = p_authors
        self.__authorsDet__      = p_authorsDet
        self.__article_date__    = p_article_date        
        self.__content__         = p_content
        
        if p_topic_keywords: 
            self.__topic_keywords__     = p_topic_keywords
            self.__topicKeywordsList__  = [item.strip() for item in p_topic_keywords.split(',')]
            
        if p_tokens: 
            self.__tokens__      = p_tokens
            self.__tokensList__  = [item.strip() for item in p_tokens.split(' | ')]
        return        
    
    def loadFromDB(self, p_session):
        v_instance = self.recordExists(p_session, __tabPK__ = self.getPK())
        if v_instance:
            if v_instance.__tokens__:
                v_instance.__tokensList__ = [item.strip() for item in v_instance.__tokens__.split(' | ')] 
            if v_instance.__topic_keywords__:
                v_instance.__topicKeywordsList__ = [item.strip() for item in v_instance.__topic_keywords__.split(' | ')] 
        return v_instance
    
    def getPK(self):
        return self.__tabPK__
    
    def getContent(self):
        return self.__content__
    
    def getTokens(self):
        return self.__tokensList__
    
    def getTopicKeywords(self):
        return self.__topicKeywordsList__
    
    def getValues(self):
        return { '__tabPK__':            self.__tabPK__,
                 '__lang__':             self.__lang__,
                 '__title__':            self.__title__,
                 '__topic_keywords__':   ' | '.join([item.replace('|', '') for item in self.__topicKeywordsList__]),
                 '__authors__':          self.__authors__,
                 '__authorsDet__':       self.__authorsDet__,                
                 '__article_date__':     self.__article_date__,                
                 '__content__':          self.__content__,
                 '__tokens__':           ' | '.join([item.replace('|', '') for item in self.__tokensList__]) }
    
    def recordIU(self, p_session, p_update = False):
        try:
            v_instance = self.recordExists(p_session, __tabPK__ = self.getPK())
            if v_instance:
                if p_update:
                    self.update(p_session, self.getPK(), self.getValues())
                    p_session.commit()
                    v_instance = self.loadFromDB(p_session)
                return v_instance
            else:
                p_session.add(self)
                p_session.commit()
                return self
        except:
            p_session.close()
            raise
    
    def printShort(self, p_context):        
        v_print = self.getValues()
        v_print.pop('__authors__',         None)
        v_print.pop('__authorsDet__',      None)
        v_print.pop('__article_date__',    None)
        v_print.pop('__content__',         None)
        v_print.pop('__tokens__',          None)
        v_print.pop('__topic_keywords__',  None)
        
        for key in v_print.keys():
            v_print[key.replace('__', '')] = v_print.pop(key)
        
        v_print['tokensSize']    = len(self.__tokensList__)
        v_print['topicKeywords'] = len(self.__topicKeywordsList__)
        
        print(p_context, v_print)
        return
    
    @classmethod
    def recordExists(p_class, p_session, **kwargs):
        v_instance = p_session.query(p_class).filter_by(**kwargs).first()
        return v_instance
    
    @classmethod
    def update(p_class, p_session, p_filter, p_values):
        p_session.query(p_class).filter_by(__tabPK__ = p_filter).update(p_values)
        return

def createTableNLPArticle(p_engine):
    try:
        NLPArticle.__table__.drop(p_engine)
    except: None
    NLPArticle.__table__.create(p_engine)
    return