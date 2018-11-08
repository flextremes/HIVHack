from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class NLPTopic(Base):
    __tablename__          = 'Topics'
    __tabPK__              = Column('tabPK',          Integer, primary_key = True)
    __topicName__          = Column('topicName',      String(30))
    __topicKeywords__      = Column('topicKeywords',  String(10000))
    __topicKeywordsList__  = []
    
    def __init__( self, p_tabPK, 
                        p_topicName       = None, 
                        p_topicKeywords   = None ):
        self.__tabPK__      = p_tabPK        
        self.__topicName__  = p_topicName
        
        if p_topicKeywords: 
            self.__topicKeywordsList__  = [item.replace('"', ' ').replace('*', ' *').strip() for item in p_topicKeywords.split('+')]
            self.__topicKeywords__      = ' | '.join([item.replace('|', '') for item in self.__topicKeywordsList__])
            
        return        
    
    def loadFromDB(self, p_session):
        v_instance = self.recordExists(p_session, __tabPK__ = self.getPK())
        if v_instance:
            if v_instance.__topicKeywords__:
                v_instance.__topicKeywordsList__ = [item.strip() for item in v_instance.__topicKeywords__.split(' | ')] 
        return v_instance
    
    def getPK(self):
        return self.__tabPK__
    
    def getKeywords(self):
        return self.__topicKeywordsList__
    
    def getValues(self):
        return { '__tabPK__':          self.__tabPK__,
                 '__topicName__':      self.__topicName__,
                 '__topicKeywords__':  ' | '.join([item.replace('|', '') for item in self.__topicKeywordsList__]) }
    
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
        v_print.pop('__topicKeywords__',  None)
        
        for key in v_print.keys():
            v_print[key.replace('__', '')] = v_print.pop(key)
        
        v_print['topicKeywordsSize']  = len(self.__topicKeywordsList__)        
        
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

def createTableNLPTopic(p_engine):
    try:
        NLPTopic.__table__.drop(p_engine)
    except: None
    NLPTopic.__table__.create(p_engine)
    return