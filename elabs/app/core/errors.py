#coding:utf-8

from elabs.fundamental.errors import ErrorEntry

error_defs = {
    0: "CTP:正确",

}




class ErrorDefs(object):
    OK                  = ErrorEntry(0,u'succ')
    Error               = ErrorEntry(1,u'未定义的错误')
    ParameterInvalid    = ErrorEntry(101,u'参数无效')
    BAR_SCALE_INVALID   = ErrorEntry(1001,u'k线时间刻度规格错误')