from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class PubDocument(Base):
    __tablename__   = 'PubDocument'
    __tabPK__       = Column('tabPK',       String(250), primary_key = True)
    __title__       = Column('title',       String(500))
    __paper_Date__  = Column('paper_Date',  String(30))
    __filename__    = Column('filename',    String(500))
    __authors__     = Column('authors',     String(1000))
    __authorsDet__  = Column('authorsDet',  String(1000))
    __lang__        = Column('lang',        String(10))
    __contentSize__ = Column('contentSize', Integer)
    __content__     = Column('content',     String(10000))
    __tokens__      = Column('tokens',      String(10000))
    __tokensList__  = []
    
    def __init__( self, p_tabPK, 
                  p_title       = None, 
                  p_paper_Date  = None, 
                  p_filename    = None, 
                  p_authors     = None, 
                  p_authorsDet  = None, 
                  p_lang        = None, 
                  p_contentSize = None, 
                  p_content     = None,
                  p_tokens      = None ):
        self.__tabPK__       = p_tabPK        
        self.__title__       = p_title
        self.__paper_Date__  = p_paper_Date
        self.__filename__    = p_filename
        self.__authors__     = p_authors
        self.__authorsDet__  = p_authorsDet
        self.__lang__        = p_lang
        self.__contentSize__ = p_contentSize
        self.__content__     = p_content
        self.__tokens__      = p_tokens
    
    def loadFromDB(self, p_session):
        v_instance = self.recordExists(p_session, __tabPK__ = self.getPK())
        if v_instance:
            if v_instance.__tokens__:
                v_instance.__tokensList__ = v_instance.__tokens__.split(' | ')
        return v_instance
    
    def getPK(self):
        return self.__tabPK__
    
    def getContent(self):
        return self.__content__
    
    def getTokens(self):
        return self.__tokens__
    
    def getTokensList(self):
        return self.__tokensList__
    
    def getValues(self):
        return { '__tabPK__':       self.__tabPK__,
                 '__title__':       self.__title__,
                 '__paper_Date__':  self.__paper_Date__,
                 '__filename__':    self.__filename__,
                 '__authors__':     self.__authors__,
                 '__authorsDet__':  self.__authorsDet__,
                 '__lang__':        self.__lang__,
                 '__contentSize__': self.__contentSize__,
                 '__content__':     self.__content__,
                 '__tokens__':      ' | '.join([item.replace('|', '') for item in self.__tokensList__]) }
    
    def updateToken(self, p_session, p_token):
        self = self.loadFromDB(p_session)        
        self.__tokensList__ = p_token            
        self.update(p_session, self.getPK(), self.getValues())
        p_session.commit()
        self = self.loadFromDB(p_session)
        return
    
    def recordIU(self, p_session, p_update = False):
        try:
            v_instance = self.recordExists(p_session, __tabPK__ = self.getPK())
            if v_instance:
                if p_update:
                    self.update(p_session, self.getPK(), self.getValues())
                    p_session.commit()
                    v_instance = self.recordExists(p_session, __tabPK__ = self.getPK())
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
        v_print.pop('__title__',       None)
        v_print.pop('__authors__',     None)
        v_print.pop('__authorsDet__',  None)
        v_print.pop('__content__',     None)
        v_print.pop('__paper_Date__',  None)
        v_print.pop('__filename__',    None)
        v_print.pop('__tokens__',      None)
        
        for key in v_print.keys():
            v_print[key.replace('__', '')] = v_print.pop(key)
        
        v_print['tokensSize'] = len(self.__tokensList__)
        
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

def createTablePubDocument(p_engine):
    try:
        PubDocument.__table__.drop(p_engine)
    except: None
    PubDocument.__table__.create(p_engine)
    return