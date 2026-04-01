export namespace main {
	
	export class ProxyResponse {
	    statusCode: number;
	    headers: Record<string, string>;
	    body: string;
	    bodyEncoding: string;
	
	    static createFrom(source: any = {}) {
	        return new ProxyResponse(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.statusCode = source["statusCode"];
	        this.headers = source["headers"];
	        this.body = source["body"];
	        this.bodyEncoding = source["bodyEncoding"];
	    }
	}

}

