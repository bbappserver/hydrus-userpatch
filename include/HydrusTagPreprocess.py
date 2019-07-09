import re
class TagPreprocessor(object):
    
    RESULT_ACCEPT=0 #Automatic acceptance
    RESULT_CHECK=1 #Check by human
    #RESULT_REJECT=2 #DON'T UNCOMMENT THIS, IT'S JUST HERE TO REMIND YOU IT WOULD BE A BAD IDEA

    HASHLIKE_NAMESPACE=("md5","sha1","sha2","sha128","sha256","sha512",)
    HASHLIKE_LENGTH=(32,64,128,512,512)
    HEX_REGEX="[a-z0-9]{32,64,128,256,512}"

    PAGE_REGEX="[a-z]?\d{1,3}"
    RANDOM_REGEX="[^a-z]{"
    SCREECHING_REGEX="(.)\1{3,}"
    TIMESTAMP_REGEX="[1-3][0-9]{3}(0[0-9]|1[0-2])?(0[1-9]|[12][0-9]|3[01])?"

    NARCISSISM=("dont","don't","i","my")
    MAX_REASONABLE_TAG_LENGTH=45

    CHECK_DESCRIPTIONS=(
    ("hashlike","Checks for tags that look like hex hashes"),
    ("narcissism","Checks for tags that look like \"don't steal my art it's mine\""),
    ("length","Checks for tags that look suspiciously long"),
    ("screech","Checks for strings that look like 'get reckttttttt'"),
    ("unnamspaced-num","Checks for numbers not in a namespace excluding those resembling years"),
    ("page-num","Checks for non-numberlike page numbers, some tollerance for letters"),
    ("chapter+volume","Checks for non numeric volumes and chapters")
    )

    def all_checks(self):
        l=[]
        for name,desc in self.CHECK_DESCRIPTIONS:
            l.append(name)
        return l 
    def __init__(self, max_length=MAX_REASONABLE_TAG_LENGTH, auto_accept=[],checks=None ):
        self.max_length=max_length  
        self.keep= auto_accept
        if checks is None:
            self.checks=self.all_checks()  
        else:
            self.checks=checks

    def resembles_hash(self,namespace,name):
        if name is None:
            raise ValueError("Name must be a string")
        if namespace in self.HASHLIKE_NAMESPACE:
            return True
        if len(name) in self.HASHLIKE_LENGTH and re.match(name,self.HEX_REGEX):
            return True
    
    def strange_page(self,namespace,name): 
        return not re.match(name,self.PAGE_REGEX)

    def evaluate_tag(self,tag):
        if tag in self.keep:
            return self.RESULT_ACCEPT
        namespace,name=tag

        #START Regardless of namespace
        if "length" in self.checks and len(name)+len(namespace) > self.max_length:
            return self.RESULT_CHECK
        if "screech" in re.search(name,self.SCREECHING_REGEX):
            return self.RESULT_CHECK
        if "narcissim" in name in self.NARCISSISM:
            return self.RESULT_CHECK
        if "hashlike" in self.resembles_hash(namespace,name):
            return self.RESULT_CHECK

        #END Regardless of namespace
        if namespace is None:
            if "unnamespaced-num" in self.checks \
                and re.match(name,r"\d+") and not re.match(name,self.TIMESTAMP_REGEX): 
                return self.RESULT_CHECK #Unnamespaced number
        elif namespace in ("page") and "page-num" in self.checks: 
            if self.strange_page(name,namespace):return self.RESULT_CHECK #non-numberish page
        elif namespace in ("chapter","volume") and "chapter+volume" in self.checks:
            if self.strange_page(name,namespace):return self.RESULT_CHECK #non-numberish
